import json
from datetime import date, timedelta
from pathlib import Path


def load_policy(path):
    policy = json.loads(Path(path).read_text(encoding='utf-8'))
    if not isinstance(policy, dict) or 'policy_id' not in policy or 'tables' not in policy:
        raise ValueError('保留规则需要 policy_id 与按表的留存窗口')
    for table, rule in policy['tables'].items():
        if not isinstance(rule.get('retention_days'), int) or rule['retention_days'] <= 0:
            raise ValueError(f'表 {table} 的留存窗口必须是正整数天数')
    return policy


def parse_clock(clock):
    try:
        return date.fromisoformat(clock)
    except ValueError:
        raise ValueError(f'评估时钟必须是 ISO 日期：{clock!r}') from None


def month_end(partition):
    year, month = (int(part) for part in partition.split('-'))
    following = date(year + (month == 12), month % 12 + 1, 1)
    return following - timedelta(days=1)


def in_window(partition, clock, retention_days):
    """分区是否在表的留存窗口内；只使用传入时钟判断。"""
    return month_end(partition) >= parse_clock(clock) - timedelta(days=retention_days)


def load_holds(path):
    bundle = json.loads(Path(path).read_text(encoding='utf-8'))
    holds = bundle.get('holds') if isinstance(bundle, dict) else None
    if not isinstance(holds, list):
        raise ValueError('保留项文件需要 holds 数组')
    seen = set()
    for hold in holds:
        if not hold.get('hold_id') or hold['hold_id'] in seen:
            raise ValueError('保留项编号缺失或重复')
        seen.add(hold['hold_id'])
        parse_clock(hold['release_clock'])
        if not isinstance(hold.get('events'), list) or not hold['events']:
            raise ValueError(f"保留项 {hold['hold_id']} 需要事件清单")
    return holds
