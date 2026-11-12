from .core import AuditDenied, BarrierError, LakeError, ObjectRetired, RestoreIncomplete


def _restore_gate(lake):
    if lake.meta('restored') == '1' and lake.meta('barriers_applied') != '1':
        raise RestoreIncomplete('备份恢复后尚未重放已完成删除的屏障，分析读取保持关闭')


def _check_barrier(lake, seq):
    floor = lake.barrier_floor()
    if seq < floor:
        raise BarrierError(f'快照 seq={seq} 落后于删除屏障基线 seq={floor}，普通读者不得继续访问')


def _snapshot_by_ref(lake, ref):
    row = lake.db.execute(
        'SELECT * FROM snapshots WHERE snapshot_id=? OR (kind=? AND label=?)',
        (ref, 'monthly', ref),
    ).fetchone()
    if not row:
        raise LakeError(f'快照 {ref} 不存在')
    return dict(row)


def _materialize(lake, snapshot):
    import json

    manifest = json.loads(snapshot['manifest']) if isinstance(snapshot['manifest'], str) else snapshot['manifest']
    details = {}
    for partition, categories in sorted(manifest['details'].items()):
        rows = []
        for object_id in sorted(categories.values()):
            rows.extend(lake.objects.read(object_id))
        details[partition] = sorted(rows, key=lambda row: row['event'])
    aggregates = {}
    for partition, object_id in sorted(manifest['aggregates'].items()):
        aggregates[partition] = lake.objects.read(object_id)
    return {'snapshot_id': snapshot['snapshot_id'], 'seq': snapshot['seq'], 'details': details, 'aggregates': aggregates}


def query_current(lake):
    """普通查询：同时看到新明细与新汇总（同一切换事务发布）。"""
    _restore_gate(lake)
    current = lake.current()
    _check_barrier(lake, current['seq'])
    snapshot = dict(lake.db.execute('SELECT * FROM snapshots WHERE snapshot_id=?', (current['snapshot_id'],)).fetchone())
    return _materialize(lake, snapshot)


def query_snapshot(lake, ref):
    """指定旧快照的普通读者：必须服从删除屏障。"""
    _restore_gate(lake)
    snapshot = _snapshot_by_ref(lake, ref)
    _check_barrier(lake, snapshot['seq'])
    return _materialize(lake, snapshot)


def read_object(lake, object_id):
    """固定旧对象的读者：对象失效后收到明确错误，而不是悄悄返回已删行。"""
    _restore_gate(lake)
    row = lake.db.execute('SELECT * FROM objects WHERE object_id=?', (object_id,)).fetchone()
    if not row:
        if lake.db.execute('SELECT 1 FROM restricted_objects WHERE object_id=?', (object_id,)).fetchone():
            raise AuditDenied('对象位于受限保留区，普通入口不可读')
        raise LakeError(f'对象 {object_id} 未登记')
    if row['state'] in ('live', 'historical'):
        return lake.objects.read(object_id)
    if row['state'] == 'retired':
        raise ObjectRetired(f'对象 {object_id} 已被删除闭包失效，禁止返回旧行')
    raise ObjectRetired(f'对象 {object_id} 已被删除闭包回收，禁止返回旧行')


def audit_read(lake, actor, token, clock):
    """受限保留区：仅本地审计授权可读，且每次访问留下记录。"""
    _restore_gate(lake)
    if not actor or token != lake.meta('audit_token'):
        raise AuditDenied('受限保留区只允许本地审计授权访问')
    rows = []
    for record in lake.db.execute("SELECT * FROM restricted_objects WHERE state='held' ORDER BY object_id"):
        rows.extend(lake.restricted.read(record['object_id']))
    holds = [dict(row) for row in lake.db.execute("SELECT * FROM holds WHERE state='active' ORDER BY hold_id")]
    lake.audit(clock, actor, 'restricted_read', {'rows': len(rows), 'holds': len(holds)})
    return {'rows': sorted(rows, key=lambda row: row['event']), 'active_holds': holds}


def audit_trail(lake):
    return [dict(row) for row in lake.db.execute('SELECT * FROM audit_log ORDER BY seq')]
