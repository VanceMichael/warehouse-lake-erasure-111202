import sqlite3
from decimal import Decimal

from .core import LakeError


def source_rows(warehouse_db):
    connection = sqlite3.connect(warehouse_db)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            "SELECT a.event, COALESCE(i.subject, '') AS subject, a.day, a.category, a.amount "
            'FROM activity a LEFT JOIN identities i ON a.alias=i.alias ORDER BY a.event'
        ).fetchall()
        return [
            {'event': row['event'], 'subject': row['subject'], 'day': row['day'], 'category': row['category'], 'amount': row['amount']}
            for row in rows
        ]
    finally:
        connection.close()


def aggregate_content(partition, rows):
    """派生指标：金额为可逆指标（按贡献可减），去重人数为不可逆指标（须由明细重算）。"""
    sums = {}
    subjects = set()
    for row in rows:
        sums[row['category']] = sums.get(row['category'], Decimal('0')) + Decimal(row['amount'])
        if row['subject']:
            subjects.add(row['subject'])
    content = [
        {'agg': 'amount_sum', 'partition': partition, 'category': category, 'value': str(sums[category])}
        for category in sorted(sums)
    ]
    content.append({'agg': 'distinct_subjects', 'partition': partition, 'value': str(len(subjects))})
    return content


def aggregate_values(content):
    values = {}
    for entry in content:
        key = (entry['agg'], entry.get('category', ''))
        values[key] = entry['value']
    return values


def register_object(lake, object_id, kind, partition, category, rows, state, connection=None):
    db = connection or lake.db
    db.execute(
        'INSERT INTO objects VALUES (?, ?, ?, ?, ?, ?) '
        'ON CONFLICT(object_id) DO UPDATE SET kind=excluded.kind, partition=excluded.partition, '
        'category=excluded.category, rows=excluded.rows, state=excluded.state',
        (object_id, kind, partition, category, rows, state),
    )


def set_object_state(lake, object_id, state, connection=None):
    db = connection or lake.db
    db.execute('UPDATE objects SET state=? WHERE object_id=?', (state, object_id))


def build(lake, warehouse_db, clock):
    """把当前明细表物化为按月分区、按类别共享的不可变对象，并重建派生指标。

    分区一旦有新行，整分区的活动对象按类别重写；压实产生的合并对象同样被展开重写。
    """
    known = {row['event'] for row in lake.db.execute('SELECT event FROM row_index')}
    fresh = [row for row in source_rows(warehouse_db) if row['event'] not in known]
    if not fresh:
        return {'new_rows': 0, 'partitions': []}
    affected = sorted({row['day'][:7] for row in fresh})
    with lake.transaction() as db:
        for partition in affected:
            previous = db.execute(
                "SELECT object_id FROM objects WHERE state='live' AND kind='detail' AND partition=?", (partition,)
            ).fetchall()
            merged = []
            for row in previous:
                merged.extend(lake.objects.read(row['object_id']))
            merged.extend(row for row in fresh if row['day'][:7] == partition)
            by_category = {}
            for row in merged:
                by_category.setdefault(row['category'], []).append(row)
            for category, rows in sorted(by_category.items()):
                rows = sorted(rows, key=lambda row: row['event'])
                object_id = lake.objects.put(rows)
                register_object(lake, object_id, 'detail', partition, category, len(rows), 'live', db)
                for row in rows:
                    db.execute(
                        'INSERT INTO row_index VALUES (?, ?, ?, ?) ON CONFLICT(event) DO UPDATE SET object_id=excluded.object_id',
                        (row['event'], object_id, row['subject'], partition),
                    )
                db.execute('DELETE FROM agg_contrib WHERE partition=? AND category=?', (partition, category))
                contrib = {}
                for row in rows:
                    amount, count = contrib.get(row['subject'], (Decimal('0'), 0))
                    contrib[row['subject']] = (amount + Decimal(row['amount']), count + 1)
                for subject, (amount, count) in sorted(contrib.items()):
                    db.execute(
                        'INSERT INTO agg_contrib VALUES (?, ?, ?, ?, ?)',
                        (subject, partition, category, count, str(amount)),
                    )
            for row in previous:
                set_object_state(lake, row['object_id'], 'historical', db)
            rows = partition_rows(lake, partition)
            content = aggregate_content(partition, rows)
            object_id = lake.objects.put(content)
            register_object(lake, object_id, 'aggregate', partition, None, len(content), 'live', db)
            stale = db.execute(
                "SELECT object_id FROM objects WHERE state='live' AND kind='aggregate' AND partition=? AND object_id!=?",
                (partition, object_id),
            ).fetchall()
            for row in stale:
                set_object_state(lake, row['object_id'], 'historical', db)
        snapshot_id, seq = lake.publish('live', None, clock, lake.manifest_from_catalog(), db)
    return {'new_rows': len(fresh), 'partitions': affected, 'snapshot': snapshot_id, 'seq': seq}


def partition_rows(lake, partition, manifest=None):
    manifest = manifest or lake.manifest_from_catalog()
    rows = []
    for object_id in manifest['details'].get(partition, {}).values():
        rows.extend(lake.objects.read(object_id))
    return sorted(rows, key=lambda row: row['event'])


def seal(lake, label, clock):
    """按月封存分析快照：清单固定当前明细对象与派生指标引用。"""
    if lake.db.execute('SELECT 1 FROM snapshots WHERE kind=? AND label=?', ('monthly', label)).fetchone():
        raise LakeError(f'月度快照 {label} 已封存，不可重复')
    snapshot_id, seq = lake.publish('monthly', label, clock, lake.manifest_from_catalog())
    return {'snapshot': snapshot_id, 'seq': seq, 'label': label}
