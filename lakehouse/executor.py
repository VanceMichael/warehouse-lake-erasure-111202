import json

from warehouse.store import digest_for

from .build import aggregate_content, register_object, set_object_state
from .core import BaselineConflict, LakeError
from .planner import load_plan


class InterruptedExecution(LakeError):
    """演示/测试注入的中断：计划保持可恢复状态。"""


def _step_rewrite(lake, plan, payload):
    for action in payload['objects']:
        if action['action'] != 'rewrite':
            continue
        source = lake.objects.read(action['object_id'])
        removed = set(action['erased_events']) | set(action['restricted_events'])
        kept = [row for row in source if row['event'] not in removed]
        if digest_for(kept) != action['new_object_id']:
            raise LakeError(f"对象 {action['object_id']} 重写结果与计划指纹不符")
        lake.objects.put(kept)
        register_object(lake, action['new_object_id'], 'detail', action['partition'], action['category'], len(kept), 'staged')


def _step_transfer(lake, plan, payload):
    spec = plan['spec']
    for restricted in payload['restricted']:
        rows = []
        for action in spec['objects']:
            wanted = [event for event in action['restricted_events'] if event in restricted['events']]
            if wanted:
                source = lake.objects.read(action['object_id'])
                rows.extend(row for row in source if row['event'] in wanted)
        rows = sorted(rows, key=lambda row: row['event'])
        if [row['event'] for row in rows] != restricted['events'] or digest_for(rows) != restricted['object_id']:
            raise LakeError('受限保留区转存内容与计划指纹不符')
        lake.restricted.put(rows)
        with lake.transaction() as db:
            db.execute(
                'INSERT OR IGNORE INTO restricted_objects VALUES (?, ?, ?, ?)',
                (restricted['object_id'], restricted['partition'], restricted['rows'], 'held'),
            )
            for event in restricted['events']:
                db.execute('INSERT OR IGNORE INTO restricted_index VALUES (?, ?)', (event, restricted['object_id']))


def _step_aggregates(lake, plan, payload):
    spec = plan['spec']
    manifest = spec['manifest']
    for action in payload['aggregates']:
        partition = action['partition']
        remaining = []
        for object_id in manifest['details'].get(partition, {}).values():
            remaining.extend(lake.objects.read(object_id))
        remaining = sorted(remaining, key=lambda row: row['event'])
        content = aggregate_content(partition, remaining)
        if digest_for(content) != action['new_object_id']:
            raise LakeError(f'分区 {partition} 汇总重建结果与计划指纹不符')
        lake.objects.put(content)
        register_object(lake, action['new_object_id'], 'aggregate', partition, None, len(content), 'staged')


def _step_switch(lake, plan, payload):
    spec = plan['spec']
    barrier_id = payload['barrier_id']
    existing = lake.db.execute('SELECT snapshot_seq FROM barriers WHERE barrier_id=?', (barrier_id,)).fetchone()
    current = lake.current()
    if current['seq'] != spec['baseline_seq']:
        if existing and current['manifest'] == payload['manifest']:
            barrier = _barrier_record(plan, spec, barrier_id, existing['snapshot_seq'])
            lake.append_journal(barrier)
            return
        raise BaselineConflict(
            f"删除切换基线过期：计划基线 seq={spec['baseline_seq']}，当前 seq={current['seq']}，需重新规划"
        )
    with lake.transaction() as db:
        _, seq = lake.publish('live', None, spec['clock'], payload['manifest'], db)
        if existing and existing['snapshot_seq'] != seq:
            raise LakeError('屏障日志序号与本地历史不一致，拒绝切换')
        barrier = _barrier_record(plan, spec, barrier_id, seq)
        lake.insert_barrier(barrier, db)
        staged = [a['new_object_id'] for a in spec['objects'] if a['new_object_id']]
        staged += [a['new_object_id'] for a in spec['aggregates']]
        for object_id in staged:
            set_object_state(lake, object_id, 'live', db)
        for object_id in payload['retire']:
            set_object_state(lake, object_id, 'retired', db)
        for event in payload['erased_events'] + payload['restricted_events']:
            db.execute('DELETE FROM row_index WHERE event=?', (event,))
        db.execute('DELETE FROM agg_contrib WHERE subject=?', (spec['subject'],))
    lake.append_journal(barrier)


def _barrier_record(plan, spec, barrier_id, snapshot_seq):
    return {
        'barrier_id': barrier_id,
        'plan_id': plan['plan_id'],
        'subject': spec['subject'],
        'clock': spec['clock'],
        'snapshot_seq': snapshot_seq,
    }


def _step_gc(lake, plan, payload):
    for object_id in payload['retire']:
        target = lake.objects.root / (object_id + '.jsonl')
        if target.exists():
            target.unlink()
        set_object_state(lake, object_id, 'collected')


def execute(lake, plan_id, fail_at=None):
    """按持久化步骤执行计划；中断后重入即恢复，已完成步骤幂等跳过。"""
    plan = load_plan(lake, plan_id)
    if plan['status'] == 'done':
        return {'plan_id': plan_id, 'status': 'done', 'resumed': False}
    if plan['status'] == 'planned':
        lake.db.execute("UPDATE plans SET status='running' WHERE plan_id=?", (plan_id,))
    handlers = {
        'rewrite': _step_rewrite,
        'transfer': _step_transfer,
        'aggregates': _step_aggregates,
        'switch': _step_switch,
        'gc': _step_gc,
    }
    ran = []
    for step in plan['steps']:
        if step['status'] == 'done':
            continue
        if fail_at == step['kind']:
            raise InterruptedExecution(f"计划在步骤 {step['kind']} 前注入中断，可重入恢复")
        payload = json.loads(step['payload'])
        handlers[step['kind']](lake, plan, payload)
        lake.db.execute("UPDATE plan_steps SET status='done' WHERE plan_id=? AND seq=?", (plan_id, step['seq']))
        ran.append(step['kind'])
    lake.db.execute("UPDATE plans SET status='done' WHERE plan_id=?", (plan_id,))
    return {'plan_id': plan_id, 'status': 'done', 'executed_steps': ran}


def credential(lake, plan_id):
    """完成凭据：只含计划指纹、计数与剩余保留理由，不复制主体内容。"""
    plan = load_plan(lake, plan_id)
    if plan['status'] != 'done':
        raise LakeError('计划尚未完成，不能签发完成凭据')
    spec = plan['spec']
    restricted_events = [event for action in spec['objects'] for event in action['restricted_events']]
    remaining = []
    if restricted_events:
        marks = ','.join('?' for _ in restricted_events)
        rows = lake.db.execute(
            f"SELECT DISTINCT h.hold_id, h.reason, h.release_clock FROM holds h "
            f"JOIN hold_rows r ON h.hold_id=r.hold_id WHERE h.state='active' AND r.event IN ({marks}) "
            'ORDER BY h.hold_id',
            restricted_events,
        ).fetchall()
        remaining = [dict(row) for row in rows]
    return {
        'plan_id': plan_id,
        'plan_fingerprint': plan['fingerprint'],
        'counts': spec['counts'],
        'remaining_holds': remaining,
    }
