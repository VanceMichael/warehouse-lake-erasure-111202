import hashlib
import json
from decimal import Decimal

from warehouse.catalog import encode
from warehouse.store import digest_for

from .build import aggregate_content, aggregate_values
from .core import LakeError
from .policy import in_window, load_policy

STEP_KINDS = ['rewrite', 'transfer', 'aggregates', 'switch', 'gc']


def active_holds(lake, clock):
    """评估时钟下仍有效的保留项：释放时刻必须严格晚于时钟。"""
    return [
        dict(row)
        for row in lake.db.execute(
            "SELECT * FROM holds WHERE state='active' AND release_clock > ? ORDER BY hold_id", (clock,)
        )
    ]


def held_events(lake, holds):
    events = set()
    for hold in holds:
        for row in lake.db.execute('SELECT event FROM hold_rows WHERE hold_id=?', (hold['hold_id'],)):
            events.add(row['event'])
    return events


def create_plan(lake, subject, clock, policy_path):
    """固定策略、源快照与评估时钟，生成该主体的删除闭包计划。"""
    policy = load_policy(policy_path)
    window = policy['tables']['activity']['retention_days']
    baseline = lake.current()
    rows = [dict(row) for row in lake.db.execute('SELECT * FROM row_index WHERE subject=? ORDER BY event', (subject,))]
    if not rows:
        raise LakeError(f'主体 {subject} 在明细中没有贡献，无需删除闭包')
    holds = active_holds(lake, clock)
    held = held_events(lake, holds)

    by_object = {}
    for row in rows:
        by_object.setdefault(row['object_id'], []).append(row['event'])

    ref_keys = {}
    for partition, categories in baseline['manifest']['details'].items():
        for key, object_id in categories.items():
            ref_keys[object_id] = (partition, key)

    object_actions = []
    restricted_by_partition = {}
    expired = []
    for object_id in sorted(by_object):
        info = lake.db.execute('SELECT * FROM objects WHERE object_id=?', (object_id,)).fetchone()
        if not info or info['state'] != 'live' or object_id not in ref_keys:
            raise LakeError(f'对象 {object_id} 不在当前快照中，计划基线不一致')
        partition, category = ref_keys[object_id]
        if not in_window(partition, clock, window):
            expired.append({'object_id': object_id, 'partition': partition, 'events': sorted(by_object[object_id])})
            continue
        content = lake.objects.read(object_id)
        subject_events = set(by_object[object_id])
        kept = [row for row in content if row['event'] not in subject_events]
        restricted = sorted(event for event in subject_events if event in held)
        erased = sorted(event for event in subject_events if event not in held)
        action = 'rewrite' if kept else 'drop'
        new_id = digest_for(kept) if kept else None
        if action == 'drop' and restricted:
            raise LakeError('内部不一致：整对象受限却无可保留行')
        object_actions.append(
            {
                'object_id': object_id,
                'partition': partition,
                'category': category,
                'action': action,
                'new_object_id': new_id,
                'rows_total': len(content),
                'rows_kept': len(kept),
                'rows_erased': len(erased),
                'rows_restricted': len(restricted),
                'erased_events': erased,
                'restricted_events': restricted,
                'shared': len(kept) > 0,
            }
        )
        for row in content:
            if row['event'] in restricted:
                restricted_by_partition.setdefault(partition, []).append(row)

    restricted_objects = []
    for partition, restricted_rows in sorted(restricted_by_partition.items()):
        restricted_rows = sorted(restricted_rows, key=lambda row: row['event'])
        restricted_objects.append(
            {
                'object_id': digest_for(restricted_rows),
                'partition': partition,
                'rows': len(restricted_rows),
                'events': [row['event'] for row in restricted_rows],
            }
        )

    manifest = json.loads(json.dumps(baseline['manifest']))
    for action in object_actions:
        categories = manifest['details'][action['partition']]
        if action['action'] == 'drop':
            del categories[action['category']]
        else:
            categories[action['category']] = action['new_object_id']

    affected_partitions = sorted({action['partition'] for action in object_actions})
    aggregate_actions = []
    for partition in affected_partitions:
        remaining = []
        for category, object_id in sorted(manifest['details'].get(partition, {}).items()):
            planned = next((a for a in object_actions if a['new_object_id'] == object_id), None)
            if planned:
                source = lake.objects.read(planned['object_id'])
                removed = set(planned['erased_events']) | set(planned['restricted_events'])
                remaining.extend(row for row in source if row['event'] not in removed)
            else:
                remaining.extend(lake.objects.read(object_id))
        remaining = sorted(remaining, key=lambda row: row['event'])
        recomputed = aggregate_values(aggregate_content(partition, remaining))
        old_object = baseline['manifest']['aggregates'][partition]
        old_values = aggregate_values(lake.objects.read(old_object))
        reversible = {}
        for row in lake.db.execute('SELECT * FROM agg_contrib WHERE partition=? AND subject=?', (partition, subject)):
            key = ('amount_sum', row['category'])
            removed = Decimal(row['amount'])
            updated = Decimal(old_values[key]) - removed
            if key not in recomputed:
                if updated != 0:
                    raise LakeError(f'分区 {partition} 可逆指标校验失败：类别移除后仍有余额')
                reversible[row['category']] = {'old': old_values[key], 'removed': str(removed), 'new': None}
                continue
            if str(updated) != recomputed[key]:
                raise LakeError(f'分区 {partition} 可逆指标校验失败：移除贡献与重算不一致')
            reversible[row['category']] = {'old': old_values[key], 'removed': str(removed), 'new': str(updated)}
        content = aggregate_content(partition, remaining)
        new_object = digest_for(content)
        aggregate_actions.append(
            {
                'partition': partition,
                'old_object_id': old_object,
                'new_object_id': new_object,
                'reversible': reversible,
                'irreversible': {
                    'old': old_values[('distinct_subjects', '')],
                    'new': recomputed[('distinct_subjects', '')],
                    'method': 'recompute-from-remaining-details',
                },
            }
        )
        manifest['aggregates'][partition] = new_object

    retire = sorted(
        {action['object_id'] for action in object_actions}
        | {action['old_object_id'] for action in aggregate_actions}
    )
    counts = {
        'subject_rows': len(rows),
        'rows_erased': sum(action['rows_erased'] for action in object_actions),
        'rows_restricted': sum(action['rows_restricted'] for action in object_actions),
        'objects_rewritten': sum(1 for action in object_actions if action['action'] == 'rewrite'),
        'objects_dropped': sum(1 for action in object_actions if action['action'] == 'drop'),
        'restricted_objects': len(restricted_objects),
        'aggregates_rebuilt': len(aggregate_actions),
        'expired_partitions': len(expired),
    }
    spec = {
        'subject': subject,
        'clock': clock,
        'policy_id': policy['policy_id'],
        'baseline_seq': baseline['seq'],
        'objects': object_actions,
        'restricted': restricted_objects,
        'aggregates': aggregate_actions,
        'expired': expired,
        'manifest': manifest,
        'retire': retire,
        'counts': counts,
    }
    fingerprint = hashlib.sha256(encode(spec)).hexdigest()
    plan_id = 'plan-' + fingerprint[:16]
    if lake.db.execute('SELECT 1 FROM plans WHERE plan_id=?', (plan_id,)).fetchone():
        raise LakeError(f'计划 {plan_id} 已存在，重复规划不会产生新计划')
    payloads = {
        'rewrite': {'objects': object_actions},
        'transfer': {'restricted': restricted_objects},
        'aggregates': {'aggregates': aggregate_actions},
        'switch': {
            'manifest': manifest,
            'barrier_id': 'barrier-' + plan_id,
            'retire': retire,
            'erased_events': sorted(event for action in object_actions for event in action['erased_events']),
            'restricted_events': sorted(event for action in object_actions for event in action['restricted_events']),
        },
        'gc': {'retire': retire},
    }
    with lake.transaction() as db:
        db.execute(
            'INSERT INTO plans VALUES (?, ?, ?, ?, ?, ?, ?)',
            (plan_id, fingerprint, subject, clock, baseline['seq'], 'planned', json.dumps(spec, ensure_ascii=False, sort_keys=True)),
        )
        for seq, kind in enumerate(STEP_KINDS):
            db.execute(
                'INSERT INTO plan_steps VALUES (?, ?, ?, ?, ?)',
                (plan_id, seq, kind, 'pending', json.dumps(payloads[kind], ensure_ascii=False, sort_keys=True)),
            )
    return plan_id


def load_plan(lake, plan_id):
    row = lake.db.execute('SELECT * FROM plans WHERE plan_id=?', (plan_id,)).fetchone()
    if not row:
        raise LakeError(f'计划 {plan_id} 不存在')
    plan = dict(row)
    plan['spec'] = json.loads(plan['spec'])
    plan['steps'] = [dict(step) for step in lake.db.execute('SELECT * FROM plan_steps WHERE plan_id=? ORDER BY seq', (plan_id,))]
    return plan


def preview(lake, plan_id):
    """删除预览：给出每个对象的行数与引用变化，以及派生指标的旧值/新值。"""
    plan = load_plan(lake, plan_id)
    spec = plan['spec']
    references = []
    for action in spec['objects']:
        references.append(
            {
                'partition': action['partition'],
                'category': action['category'],
                'old_object': action['object_id'],
                'new_object': action['new_object_id'] if action['action'] == 'rewrite' else None,
                'action': action['action'],
                'rows_total': action['rows_total'],
                'rows_kept': action['rows_kept'],
                'rows_erased': action['rows_erased'],
                'rows_restricted': action['rows_restricted'],
                'shared_object': action['shared'],
            }
        )
    aggregates = []
    for action in spec['aggregates']:
        aggregates.append(
            {
                'partition': action['partition'],
                'old_object': action['old_object_id'],
                'new_object': action['new_object_id'],
                'reversible': action['reversible'],
                'irreversible': action['irreversible'],
            }
        )
    return {
        'plan_id': plan_id,
        'fingerprint': plan['fingerprint'],
        'subject': spec['subject'],
        'clock': spec['clock'],
        'policy_id': spec['policy_id'],
        'baseline_seq': spec['baseline_seq'],
        'counts': spec['counts'],
        'object_references': references,
        'restricted_zone': spec['restricted'],
        'aggregate_changes': aggregates,
        'expired_partitions': spec['expired'],
    }
