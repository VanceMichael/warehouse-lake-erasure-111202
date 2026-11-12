import json
import tempfile
import unittest
from pathlib import Path

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
from lakehouse.planner import create_plan, load_plan, preview
from lakehouse.reads import audit_read, audit_trail, query_current, query_snapshot, read_object
from warehouse.catalog import connect, load_bundle

ROOT = Path(__file__).resolve().parents[1]
SOURCE = json.loads((ROOT / 'examples/source.json').read_text(encoding='utf-8'))
POLICY = ROOT / 'examples/retention_rules.json'
HOLDS = [
    {'hold_id': 'HOLD-A', 'reason': '合成证据保留-交易核对', 'release_clock': '2026-12-01', 'events': ['e05', 'e07']},
    {'hold_id': 'HOLD-B', 'reason': '合成证据保留-争议复核', 'release_clock': '2027-01-15', 'events': ['e07']},
]
SUBJECT = 'synthetic-1'
CLOCK = '2026-11-12'


class LakeFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        work = Path(self.temporary.name)
        self.warehouse = work / 'warehouse.sqlite3'
        connection = connect(self.warehouse)
        self.lake = Lake(work / 'lake')
        for index, month in enumerate(['2026-09', '2026-10', '2026-11']):
            bundle = {
                'identities': SOURCE['identities'] if index == 0 else [],
                'activity': [row for row in SOURCE['activity'] if row['day'].startswith(month)],
            }
            load_bundle(connection, bundle, f'batch-{month}')
            build(self.lake, self.warehouse, f'{month}-28')
            seal(self.lake, month, f'{month}-28')
        connection.close()
        register_holds(self.lake, HOLDS)

    def tearDown(self):
        self.lake.close()
        self.temporary.cleanup()

    def plan(self, clock=CLOCK, policy=POLICY, subject=SUBJECT):
        return create_plan(self.lake, subject, clock, policy)

    def flat_details(self, query):
        return [row for rows in query['details'].values() for row in rows]


class BuildTests(LakeFixture):
    def test_shared_objects_and_contributions(self):
        manifest = self.lake.current()['manifest']
        october_read = self.lake.objects.read(manifest['details']['2026-10']['read'])
        subjects = {row['subject'] for row in october_read}
        self.assertIn(SUBJECT, subjects)
        self.assertIn('synthetic-2', subjects)
        contrib = self.lake.db.execute(
            "SELECT amount FROM agg_contrib WHERE subject=? AND partition='2026-10' AND category='read'", (SUBJECT,)
        ).fetchone()
        self.assertEqual(contrib['amount'], '3.50')

    def test_monthly_snapshots_are_sealed(self):
        for month in ['2026-09', '2026-10', '2026-11']:
            snapshot = query_snapshot(self.lake, month)
            self.assertTrue(snapshot['snapshot_id'])
        september = query_snapshot(self.lake, '2026-09')
        self.assertEqual(sorted(september['details']), ['2026-09'])

    def test_rebuild_is_incremental(self):
        result = build(self.lake, self.warehouse, '2026-11-30')
        self.assertEqual(result['new_rows'], 0)


class PlanTests(LakeFixture):
    def test_preview_row_counts_and_reference_changes(self):
        plan_id = self.plan()
        view = preview(self.lake, plan_id)
        self.assertEqual(view['counts']['rows_erased'], 4)
        self.assertEqual(view['counts']['rows_restricted'], 2)
        self.assertEqual(view['counts']['objects_dropped'], 1)
        actions = {ref['partition'] + '/' + ref['category']: ref for ref in view['object_references']}
        self.assertEqual(actions['2026-11/purchase']['action'], 'drop')
        self.assertEqual(actions['2026-09/read']['rows_kept'], 1)
        self.assertTrue(all(ref['old_object'] for ref in view['object_references']))
        rewritten = [ref for ref in view['object_references'] if ref['action'] == 'rewrite']
        self.assertTrue(all(ref['new_object'] and ref['new_object'] != ref['old_object'] for ref in rewritten))

    def test_shared_object_never_dropped(self):
        plan_id = self.plan()
        view = preview(self.lake, plan_id)
        shared = [ref for ref in view['object_references'] if ref['shared_object']]
        self.assertTrue(shared)
        self.assertTrue(all(ref['action'] == 'rewrite' for ref in shared))

    def test_expired_partitions_excluded_by_window(self):
        policy = Path(self.temporary.name) / 'tight.json'
        policy.write_text(json.dumps({'policy_id': 'tight', 'tables': {'activity': {'retention_days': 30}}}), encoding='utf-8')
        plan_id = self.plan(policy=policy)
        view = preview(self.lake, plan_id)
        expired_partitions = {item['partition'] for item in view['expired_partitions']}
        self.assertEqual(expired_partitions, {'2026-09'})
        self.assertEqual({ref['partition'] for ref in view['object_references']}, {'2026-10', '2026-11'})

    def test_unknown_subject_rejected(self):
        with self.assertRaises(Exception):
            self.plan(subject='synthetic-unknown')

    def test_plan_is_persistent_and_deterministic(self):
        first = self.plan()
        with self.assertRaises(Exception):
            self.plan()
        plan = load_plan(self.lake, first)
        self.assertEqual([step['kind'] for step in plan['steps']], ['rewrite', 'transfer', 'aggregates', 'switch', 'gc'])


class ExecutionTests(LakeFixture):
    def test_full_closure(self):
        plan_id = self.plan()
        old_object = self.lake.current()['manifest']['details']['2026-10']['read']
        execute(self.lake, plan_id)
        after = query_current(self.lake)
        flat = [row for rows in after['details'].values() for row in rows]
        self.assertTrue(all(row['subject'] != SUBJECT for row in flat))
        self.assertNotIn('e05', {row['event'] for row in flat})
        self.assertNotIn('e07', {row['event'] for row in flat})
        with self.assertRaises(BarrierError):
            query_snapshot(self.lake, '2026-10')
        with self.assertRaises(ObjectRetired):
            read_object(self.lake, old_object)
        aggregates = {
            (partition, entry['agg'], entry.get('category', '')): entry['value']
            for partition, content in after['aggregates'].items()
            for entry in content
        }
        self.assertEqual(aggregates[('2026-10', 'amount_sum', 'purchase')], '30.00')
        self.assertEqual(aggregates[('2026-10', 'distinct_subjects', '')], '2')
        self.assertNotIn(('2026-11', 'amount_sum', 'purchase'), aggregates)

    def test_other_subjects_unchanged(self):
        def amounts(query):
            from decimal import Decimal

            totals = {}
            for rows in query['details'].values():
                for row in rows:
                    if row['subject'] == SUBJECT:
                        continue
                    key = (row['subject'], row['day'][:7], row['category'])
                    totals[key] = totals.get(key, Decimal('0')) + Decimal(row['amount'])
            return {key: str(value) for key, value in totals.items()}

        before = amounts(query_current(self.lake))
        execute(self.lake, self.plan())
        after = amounts(query_current(self.lake))
        self.assertEqual(before, after)

    def test_resume_after_each_step(self):
        for step in ['rewrite', 'transfer', 'aggregates', 'switch', 'gc']:
            self.tearDown()
            self.setUp()
            plan_id = self.plan()
            with self.assertRaises(InterruptedExecution):
                execute(self.lake, plan_id, fail_at=step)
            result = execute(self.lake, plan_id)
            self.assertEqual(result['status'], 'done')
            flat = [row for rows in query_current(self.lake)['details'].values() for row in rows]
            self.assertTrue(all(row['subject'] != SUBJECT for row in flat))

    def test_switch_baseline_conflict(self):
        plan_id = self.plan()
        compact = compact_prepare(self.lake, '2026-11', CLOCK)
        compact_commit(self.lake, compact['token'], CLOCK)
        with self.assertRaises(BaselineConflict):
            execute(self.lake, plan_id)

    def test_credential_hygiene(self):
        plan_id = self.plan()
        execute(self.lake, plan_id)
        final = credential(self.lake, plan_id)
        self.assertNotIn(SUBJECT, json.dumps(final, ensure_ascii=False))
        self.assertEqual(final['counts']['rows_erased'], 4)
        self.assertEqual({hold['hold_id'] for hold in final['remaining_holds']}, {'HOLD-A', 'HOLD-B'})


class RestrictedZoneTests(LakeFixture):
    def test_audit_gate_and_trail(self):
        plan_id = self.plan()
        execute(self.lake, plan_id)
        with self.assertRaises(AuditDenied):
            audit_read(self.lake, 'auditor', 'wrong', CLOCK)
        with self.assertRaises(AuditDenied):
            read_object(self.lake, load_plan(self.lake, plan_id)['spec']['restricted'][0]['object_id'])
        result = audit_read(self.lake, 'auditor', 'local-audit', CLOCK)
        self.assertEqual([row['event'] for row in result['rows']], ['e05', 'e07'])
        self.assertEqual(len(audit_trail(self.lake)), 1)

    def test_overlapping_holds_release_by_clock_only(self):
        execute(self.lake, self.plan())
        early = release_holds(self.lake, '2026-11-30')
        self.assertEqual((early['destroyed_rows'], early['retained_rows']), (0, 2))
        first = release_holds(self.lake, '2026-12-01')
        self.assertEqual(first['released_holds'], ['HOLD-A'])
        self.assertEqual((first['destroyed_rows'], first['retained_rows']), (1, 1))
        remaining = audit_read(self.lake, 'auditor', 'local-audit', '2026-12-02')
        self.assertEqual([row['event'] for row in remaining['rows']], ['e07'])
        second = release_holds(self.lake, '2027-01-15')
        self.assertEqual((second['destroyed_rows'], second['retained_rows']), (1, 0))

    def test_restricted_rows_never_in_normal_query(self):
        execute(self.lake, self.plan())
        for ref in ['2026-09', '2026-10', '2026-11']:
            with self.assertRaises(BarrierError):
                query_snapshot(self.lake, ref)
        current = query_current(self.lake)
        self.assertNotIn('e05', {row['event'] for rows in current['details'].values() for row in rows})


class CompactionTests(LakeFixture):
    def test_commit_after_deletion_conflict_then_success(self):
        prepared = compact_prepare(self.lake, '2026-09', CLOCK)
        execute(self.lake, self.plan())
        with self.assertRaises(BaselineConflict):
            compact_commit(self.lake, prepared['token'], CLOCK)
        fresh = compact_prepare(self.lake, '2026-09', CLOCK)
        committed = compact_commit(self.lake, fresh['token'], CLOCK)
        self.assertTrue(committed['snapshot'])
        current = query_current(self.lake)
        september = current['details']['2026-09']
        self.assertEqual([row['event'] for row in september], ['e02', 'e03'])


class RestoreTests(LakeFixture):
    def test_restore_reapplies_barriers_before_reads(self):
        work = Path(self.temporary.name)
        backup(self.lake, work / 'backup')
        plan_id = self.plan()
        execute(self.lake, plan_id)
        restored = restore(work / 'backup', work / 'restored')
        try:
            with self.assertRaises(RestoreIncomplete):
                query_current(restored)
            reapply_barriers(restored, self.lake.journal_path)
            with self.assertRaises(BarrierError):
                query_current(restored)
            replayed = create_plan(restored, SUBJECT, CLOCK, POLICY)
            self.assertEqual(replayed, plan_id)
            execute(restored, replayed)
            flat = [row for rows in query_current(restored)['details'].values() for row in rows]
            self.assertTrue(all(row['subject'] != SUBJECT for row in flat))
        finally:
            restored.close()


if __name__ == '__main__':
    unittest.main()
