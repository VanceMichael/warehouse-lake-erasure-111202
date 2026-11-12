import hashlib
import json
import shutil
import sqlite3
from pathlib import Path

from warehouse.catalog import encode

from .build import register_object, set_object_state
from .core import BaselineConflict, LakeError


def register_holds(lake, holds):
    with lake.transaction() as db:
        for hold in holds:
            db.execute(
                'INSERT INTO holds VALUES (?, ?, ?, ?) ON CONFLICT(hold_id) DO NOTHING',
                (hold['hold_id'], hold['reason'], hold['release_clock'], 'active'),
            )
            for event in hold['events']:
                db.execute('INSERT OR IGNORE INTO hold_rows VALUES (?, ?)', (hold['hold_id'], event))
    return len(holds)


def release_holds(lake, clock):
    """按传入时钟释放到期保留项；被多个保留项覆盖的行只有在全部释放后才销毁。"""
    released = [
        dict(row)
        for row in lake.db.execute(
            "SELECT * FROM holds WHERE state='active' AND release_clock <= ? ORDER BY hold_id", (clock,)
        )
    ]
    if not released:
        return {'released_holds': [], 'destroyed_rows': 0, 'retained_rows': _retained_count(lake)}
    with lake.transaction() as db:
        for hold in released:
            db.execute("UPDATE holds SET state='released' WHERE hold_id=?", (hold['hold_id'],))
    active = {
        row['event']
        for row in lake.db.execute(
            "SELECT r.event FROM hold_rows r JOIN holds h ON r.hold_id=h.hold_id WHERE h.state='active'"
        )
    }
    destroyed = 0
    objects = [dict(row) for row in lake.db.execute("SELECT * FROM restricted_objects WHERE state='held'")]
    for record in objects:
        rows = lake.restricted.read(record['object_id'])
        kept = [row for row in rows if row['event'] in active]
        removed = [row for row in rows if row['event'] not in active]
        if not removed:
            continue
        destroyed += len(removed)
        with lake.transaction() as db:
            for row in removed:
                db.execute('DELETE FROM restricted_index WHERE event=?', (row['event'],))
            if kept:
                new_id = lake.restricted.put(kept)
                db.execute(
                    'INSERT OR IGNORE INTO restricted_objects VALUES (?, ?, ?, ?)',
                    (new_id, record['partition'], len(kept), 'held'),
                )
                for row in kept:
                    db.execute('UPDATE restricted_index SET object_id=? WHERE event=?', (new_id, row['event']))
            db.execute("UPDATE restricted_objects SET state='collected' WHERE object_id=?", (record['object_id'],))
        target = lake.restricted.root / (record['object_id'] + '.jsonl')
        if target.exists():
            target.unlink()
    lake.audit(clock, 'retention-executor', 'hold_release', {'released': [h['hold_id'] for h in released], 'destroyed_rows': destroyed})
    return {
        'released_holds': [hold['hold_id'] for hold in released],
        'destroyed_rows': destroyed,
        'retained_rows': _retained_count(lake),
    }


def _retained_count(lake):
    return lake.db.execute('SELECT COUNT(*) FROM restricted_index').fetchone()[0]


def compact_prepare(lake, partition, clock):
    """压实准备：合并分区当前明细对象，记录基线（当前快照序号与屏障计数）。"""
    current = lake.current()
    objects = current['manifest']['details'].get(partition)
    if not objects:
        raise LakeError(f'分区 {partition} 没有可压实的明细对象')
    rows = []
    for object_id in sorted(objects.values()):
        rows.extend(lake.objects.read(object_id))
    rows = sorted(rows, key=lambda row: row['event'])
    object_id = lake.objects.put(rows)
    register_object(lake, object_id, 'detail', partition, None, len(rows), 'staged')
    token = 'compact-' + hashlib.sha256(encode({'partition': partition, 'object': object_id, 'seq': current['seq']})).hexdigest()[:16]
    lake.db.execute(
        'INSERT OR REPLACE INTO compactions VALUES (?, ?, ?, ?, ?, ?)',
        (token, partition, current['seq'], lake.barrier_count(), object_id, 'prepared'),
    )
    return {'token': token, 'partition': partition, 'object_id': object_id, 'baseline_seq': current['seq']}


def compact_commit(lake, token, clock):
    """压实提交：校验基线未变，避免压实结果重新引用已被删除闭包淘汰的对象。"""
    record = lake.db.execute('SELECT * FROM compactions WHERE token=?', (token,)).fetchone()
    if not record or record['status'] != 'prepared':
        raise LakeError(f'压实令牌 {token} 不存在或已处理')
    current = lake.current()
    if current['seq'] != record['baseline_seq'] or lake.barrier_count() != record['barrier_count']:
        lake.db.execute("UPDATE compactions SET status='aborted' WHERE token=?", (token,))
        raise BaselineConflict(
            f"压实基线已变化（准备时 seq={record['baseline_seq']}，当前 seq={current['seq']}），拒绝提交，请重新压实"
        )
    partition = record['partition']
    with lake.transaction() as db:
        manifest = json.loads(json.dumps(current['manifest']))
        old_objects = manifest['details'].get(partition, {})
        manifest['details'][partition] = {'merged': record['object_id']}
        snapshot_id, seq = lake.publish('live', None, clock, manifest, db)
        set_object_state(lake, record['object_id'], 'live', db)
        for object_id in old_objects.values():
            set_object_state(lake, object_id, 'historical', db)
            db.execute('UPDATE row_index SET object_id=? WHERE object_id=?', (record['object_id'], object_id))
        db.execute("UPDATE compactions SET status='committed' WHERE token=?", (token,))
    return {'token': token, 'snapshot': snapshot_id, 'seq': seq}


def backup(lake, dest):
    """备份目录包含目录库、对象与屏障日志；恢复时仍需显式重放屏障。"""
    dest = Path(dest)
    if dest.exists():
        raise LakeError(f'备份目标 {dest} 已存在')
    dest.mkdir(parents=True)
    target = sqlite3.connect(dest / 'lake.sqlite3')
    try:
        lake.db.backup(target)
    finally:
        target.close()
    for name in ('objects', 'restricted'):
        source = lake.root / name
        if source.exists():
            shutil.copytree(source, dest / name)
    if lake.journal_path.exists():
        shutil.copy2(lake.journal_path, dest / 'barriers.jsonl')
    return {'backup': str(dest)}


def restore(src, dest):
    """恢复备份：在重放删除屏障之前，所有分析读取保持关闭。"""
    from .core import Lake

    src = Path(src)
    dest = Path(dest)
    if dest.exists():
        raise LakeError(f'恢复目标 {dest} 已存在')
    shutil.copytree(src, dest)
    lake = Lake(dest)
    lake.set_meta('restored', '1')
    lake.set_meta('barriers_applied', '0')
    return lake


def reapply_barriers(lake, journal_path):
    """把已完成删除的屏障日志重放进恢复实例，然后才允许开放分析读取。"""
    journal_path = Path(journal_path)
    applied = 0
    if journal_path.exists():
        for line in journal_path.read_text(encoding='utf-8').splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            before = lake.barrier_count()
            lake.insert_barrier(record)
            if lake.barrier_count() > before:
                applied += 1
    lake.set_meta('barriers_applied', '1')
    return {'barriers_applied': applied, 'barrier_floor': lake.barrier_floor()}
