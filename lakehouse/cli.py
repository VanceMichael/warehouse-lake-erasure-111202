import argparse
import json
import sys

from . import build as build_mod
from . import executor, maintenance, planner, reads
from .core import Lake, LakeError
from .policy import load_holds


def main():
    parser = argparse.ArgumentParser(description='湖仓保留策略执行器（本地合成数据）')
    parser.add_argument('command')
    parser.add_argument('--lake')
    parser.add_argument('--warehouse')
    parser.add_argument('--clock')
    parser.add_argument('--label')
    parser.add_argument('--file')
    parser.add_argument('--subject')
    parser.add_argument('--policy')
    parser.add_argument('--plan')
    parser.add_argument('--fail-at')
    parser.add_argument('--snapshot')
    parser.add_argument('--object')
    parser.add_argument('--actor')
    parser.add_argument('--token')
    parser.add_argument('--partition')
    parser.add_argument('--to')
    parser.add_argument('--from', dest='source')
    parser.add_argument('--journal')
    args = parser.parse_args()

    try:
        result = dispatch(args)
    except LakeError as error:
        print(json.dumps({'error': type(error).__name__, 'message': str(error)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(2)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))


def dispatch(args):
    if args.command == 'restore':
        lake = maintenance.restore(args.source, args.to)
        lake.close()
        return {'restored': args.to, 'reads': 'closed-until-barriers-reapplied'}
    lake = Lake(args.lake)
    try:
        return run(lake, args)
    finally:
        lake.close()


def run(lake, args):
    command = args.command
    if command == 'build':
        return build_mod.build(lake, args.warehouse, args.clock)
    if command == 'seal':
        return build_mod.seal(lake, args.label, args.clock)
    if command == 'holds':
        return {'registered': maintenance.register_holds(lake, load_holds(args.file))}
    if command == 'plan':
        return {'plan_id': planner.create_plan(lake, args.subject, args.clock, args.policy)}
    if command == 'preview':
        return planner.preview(lake, args.plan)
    if command == 'execute':
        return executor.execute(lake, args.plan, fail_at=args.fail_at)
    if command == 'credential':
        return executor.credential(lake, args.plan)
    if command == 'query':
        if args.snapshot:
            return reads.query_snapshot(lake, args.snapshot)
        return reads.query_current(lake)
    if command == 'read-object':
        return {'rows': reads.read_object(lake, args.object)}
    if command == 'audit-read':
        return reads.audit_read(lake, args.actor, args.token, args.clock)
    if command == 'audit-trail':
        return reads.audit_trail(lake)
    if command == 'release-holds':
        return maintenance.release_holds(lake, args.clock)
    if command == 'compact-prepare':
        return maintenance.compact_prepare(lake, args.partition, args.clock)
    if command == 'compact-commit':
        return maintenance.compact_commit(lake, args.token, args.clock)
    if command == 'backup':
        return maintenance.backup(lake, args.to)
    if command == 'reapply-barriers':
        return maintenance.reapply_barriers(lake, args.journal)
    raise LakeError(f'未知命令 {command}')
