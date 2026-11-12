"""湖仓保留策略执行器演示：共享对象、不同释放日、跨月快照、旧对象读者、切换中断。

全部使用本地合成数据，逐项验证其他主体金额与人数不变，并证明普通入口不再暴露被删主体。
"""
import json
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lakehouse.build import build, seal
from lakehouse.core import AuditDenied, BarrierError, BaselineConflict, Lake, ObjectRetired, RestoreIncomplete
from lakehouse.executor import InterruptedExecution, credential, execute
from lakehouse.maintenance import (
    backup,
    compact_commit,
    compact_prepare,
    reapply_barriers,
    register_holds,
    release_holds,
    restore,
)
from lakehouse.planner import create_plan, preview
from lakehouse.policy import load_holds
from lakehouse.reads import audit_read, audit_trail, query_current, query_snapshot, read_object
from warehouse.catalog import connect, load_bundle

SOURCE = json.loads((ROOT / 'examples/source.json').read_text(encoding='utf-8'))
SUBJECT = 'synthetic-1'
CLOCK = '2026-11-12'


def banner(text):
    print(f'\n=== {text} ===', flush=True)


def check(condition, message):
    if not condition:
        raise RuntimeError(f'验证失败：{message}')
    print(f'  ✓ {message}', flush=True)


def expect(error, action, message):
    try:
        action()
    except error as exc:
        print(f'  ✓ {message}：{type(exc).__name__}({exc})', flush=True)
        return
    raise RuntimeError(f'验证失败：{message}（未抛出 {error.__name__}）')


def subject_amounts(details):
    totals = {}
    for rows in details.values():
        for row in rows:
            key = (row['subject'], row['day'][:7], row['category'])
            totals[key] = totals.get(key, Decimal('0')) + Decimal(row['amount'])
    return {key: str(value) for key, value in totals.items()}


def aggregate_map(aggregates):
    result = {}
    for partition, content in aggregates.items():
        for entry in content:
            result[(partition, entry['agg'], entry.get('category', ''))] = entry['value']
    return result


def main():
    temporary = tempfile.TemporaryDirectory()
    work = Path(temporary.name)
    warehouse_db = work / 'warehouse.sqlite3'
    connection = connect(warehouse_db)

    banner('建仓：按月导入合成批次并封存跨月快照')
    lake = Lake(work / 'lake')
    months = ['2026-09', '2026-10', '2026-11']
    for index, month in enumerate(months):
        bundle = {
            'identities': SOURCE['identities'] if index == 0 else [],
            'activity': [row for row in SOURCE['activity'] if row['day'].startswith(month)],
        }
        load_bundle(connection, bundle, f'batch-{month}')
        built = build(lake, warehouse_db, f'{month}-28')
        sealed = seal(lake, month, f'{month}-28')
        print(f'  {month}: 物化 {built["new_rows"]} 行 -> 快照 {sealed["snapshot"]}', flush=True)
    connection.close()

    banner('登记证据保留项（独立编号、不同释放时刻、同一行重叠覆盖）')
    register_holds(lake, load_holds(ROOT / 'examples/holds.json'))
    print('  HOLD-A 释放日 2026-12-01 覆盖 e05,e07；HOLD-B 释放日 2027-01-15 覆盖 e07', flush=True)

    baseline_query = query_current(lake)
    baseline_amounts = subject_amounts(baseline_query['details'])
    baseline_aggregates = aggregate_map(baseline_query['aggregates'])
    others_before = {key: value for key, value in baseline_amounts.items() if key[0] != SUBJECT}

    banner('固定两类读者的旧引用（旧月度快照、旧对象）')
    old_snapshot = '2026-10'
    old_object = lake.current()['manifest']['details']['2026-10']['read']
    print(f'  读者A 固定月度快照 {old_snapshot}；读者B 固定对象 {old_object[:16]}…', flush=True)

    banner('删除前备份（用于恢复后重放屏障的演示）')
    backup(lake, work / 'backup')
    print('  备份完成', flush=True)

    banner(f'规划删除闭包：主体 {SUBJECT}，评估时钟 {CLOCK}')
    plan_id = create_plan(lake, SUBJECT, CLOCK, ROOT / 'examples/retention_rules.json')
    plan_preview = preview(lake, plan_id)
    print(json.dumps(plan_preview['counts'], ensure_ascii=False, sort_keys=True), flush=True)
    for ref in plan_preview['object_references']:
        new = (ref['new_object'] or '∅')[:12]
        print(
            f"  {ref['partition']}/{ref['category']}: {ref['action']} "
            f"{ref['old_object'][:12]}… -> {new}… "
            f"总行{ref['rows_total']} 保留{ref['rows_kept']} 清除{ref['rows_erased']} 转受限{ref['rows_restricted']}",
            flush=True,
        )
    check(any(r['shared_object'] for r in plan_preview['object_references']), '存在共享对象：只重写不整块删除')
    check(any(r['action'] == 'drop' for r in plan_preview['object_references']), '独占对象允许整体移除')
    check(plan_preview['counts']['rows_restricted'] == 2, '证据保留期内的 2 行移入受限保留区而非物理销毁')

    banner('压实与删除并发：先准备压实，再执行删除')
    prepared = compact_prepare(lake, '2026-09', CLOCK)
    print(f"  压实已准备，基线 seq={prepared['baseline_seq']}", flush=True)

    banner('执行计划并在切换步骤注入中断，随后恢复')
    try:
        execute(lake, plan_id, fail_at='switch')
        raise RuntimeError('注入中断未生效')
    except InterruptedExecution as exc:
        print(f'  中断：{exc}', flush=True)
    midway = query_current(lake)
    check(any(SUBJECT == row['subject'] for rows in midway['details'].values() for row in rows), '切换前普通查询仍是旧状态（原子切换未半成品）')
    result = execute(lake, plan_id)
    check(result['status'] == 'done' and result['executed_steps'] == ['switch', 'gc'], '重入后续跑剩余步骤并完成')

    banner('压实提交必须校验基线')
    expect(BaselineConflict, lambda: compact_commit(lake, prepared['token'], CLOCK), '旧基线压实被拒绝，避免重新引用旧对象')
    fresh = compact_prepare(lake, '2026-09', CLOCK)
    committed = compact_commit(lake, fresh['token'], CLOCK)
    check(committed['seq'] > 0, '新基线压实提交成功')

    banner('验证普通入口不再暴露该主体，且其他主体金额与人数不变')
    after = query_current(lake)
    flat = [row for rows in after['details'].values() for row in rows]
    check(all(row['subject'] != SUBJECT for row in flat), '当前明细无任何该主体行')
    check(not any(row['event'] in ('e05', 'e07') for row in flat), '受限行也不出现在普通明细')
    others_after = {key: value for key, value in subject_amounts(after['details']).items() if key[0] != SUBJECT}
    check(others_after == others_before, '其他主体逐分区逐类别金额完全一致')
    aggregates_after = aggregate_map(after['aggregates'])
    expected_aggregates = {
        ('2026-09', 'amount_sum', 'read'): '5.00',
        ('2026-09', 'amount_sum', 'purchase'): '20.00',
        ('2026-09', 'distinct_subjects', ''): '1',
        ('2026-10', 'amount_sum', 'read'): '3.00',
        ('2026-10', 'amount_sum', 'purchase'): '30.00',
        ('2026-10', 'distinct_subjects', ''): '2',
        ('2026-11', 'amount_sum', 'read'): '10.00',
        ('2026-11', 'distinct_subjects', ''): '2',
    }
    check(aggregates_after == expected_aggregates, '可逆指标按贡献移除、去重人数由剩余明细重算，结果与预期一致')
    check(baseline_aggregates[('2026-10', 'distinct_subjects', '')] == '3', '删除前 2026-10 去重人数为 3（对照）')

    banner('旧引用读者必须收到明确错误')
    expect(BarrierError, lambda: query_snapshot(lake, old_snapshot), '旧月度快照读者被删除屏障拒绝')
    expect(ObjectRetired, lambda: read_object(lake, old_object), '固定旧对象的读者收到失效错误而非旧行')

    banner('受限保留区：与普通快照引用隔离，仅本地审计授权可读且留痕')
    restricted_id = plan_preview['restricted_zone'][0]['object_id']
    expect(AuditDenied, lambda: read_object(lake, restricted_id), '受限对象不进入普通对象入口')
    expect(AuditDenied, lambda: audit_read(lake, 'auditor-1', 'wrong-token', CLOCK), '错误令牌被拒绝')
    restricted = audit_read(lake, 'auditor-1', 'local-audit', CLOCK)
    check([row['event'] for row in restricted['rows']] == ['e05', 'e07'], '审计授权可读受限行 e05,e07')
    check(len(audit_trail(lake)) >= 1, '访问已写入审计记录')

    banner('保留项释放只依据传入时钟；重叠覆盖的行不得误清除')
    midway_credential = credential(lake, plan_id)
    check(
        {hold['hold_id'] for hold in midway_credential['remaining_holds']} == {'HOLD-A', 'HOLD-B'},
        '完成凭据列出剩余保留理由（HOLD-A、HOLD-B）',
    )
    outcome = release_holds(lake, '2026-11-30')
    check(outcome['destroyed_rows'] == 0 and outcome['retained_rows'] == 2, '释放日前一天：两行都保留')
    outcome = release_holds(lake, '2026-12-01')
    check(outcome['released_holds'] == ['HOLD-A'] and outcome['destroyed_rows'] == 1, 'HOLD-A 到期：e05 物理销毁')
    check(outcome['retained_rows'] == 1, 'e07 仍被 HOLD-B 覆盖，不得误清除')
    outcome = release_holds(lake, '2027-01-15')
    check(outcome['retained_rows'] == 0 and outcome['destroyed_rows'] == 1, 'HOLD-B 到期：e07 销毁，受限区清空')

    banner('备份恢复：开放分析读取前必须重放已完成删除的屏障')
    restored = restore(work / 'backup', work / 'restored')
    expect(RestoreIncomplete, lambda: query_current(restored), '未重放屏障前读取保持关闭')
    applied = reapply_barriers(restored, lake.journal_path)
    check(applied['barrier_floor'] > 0, f"屏障已重放（基线 seq={applied['barrier_floor']}）")
    expect(BarrierError, lambda: query_current(restored), '恢复实例的旧当前快照同样服从屏障')
    replayed = create_plan(restored, SUBJECT, CLOCK, ROOT / 'examples/retention_rules.json')
    check(replayed == plan_id, '恢复实例上重放规划得到相同计划指纹')
    execute(restored, replayed)
    restored_after = query_current(restored)
    check(
        all(row['subject'] != SUBJECT for rows in restored_after['details'].values() for row in rows),
        '恢复实例重放删除后普通查询不再暴露该主体',
    )
    restored.close()

    banner('完成凭据：只含计划指纹、计数与剩余保留理由')
    final = credential(lake, plan_id)
    print(json.dumps(final, ensure_ascii=False, sort_keys=True), flush=True)
    check(SUBJECT not in json.dumps(final, ensure_ascii=False), '凭据不复制主体内容')
    check(final['remaining_holds'] == [], '保留项全部释放后无剩余保留理由')

    lake.close()
    print('\n全部验证通过', flush=True)
    temporary.cleanup()


if __name__ == '__main__':
    main()
