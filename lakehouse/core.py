import json
import sqlite3
from pathlib import Path

from warehouse.catalog import transaction
from warehouse.store import ObjectStore

TABLES = {
    'meta': 'key TEXT PRIMARY KEY, value TEXT NOT NULL',
    'objects': 'object_id TEXT PRIMARY KEY, kind TEXT NOT NULL, partition TEXT, category TEXT, rows INTEGER NOT NULL, state TEXT NOT NULL',
    'row_index': 'event TEXT PRIMARY KEY, object_id TEXT NOT NULL, subject TEXT, partition TEXT NOT NULL',
    'agg_contrib': 'subject TEXT NOT NULL, partition TEXT NOT NULL, category TEXT NOT NULL, rows INTEGER NOT NULL, amount TEXT NOT NULL, PRIMARY KEY (subject, partition, category)',
    'snapshots': 'snapshot_id TEXT PRIMARY KEY, seq INTEGER NOT NULL, kind TEXT NOT NULL, label TEXT, sealed_clock TEXT NOT NULL, manifest TEXT NOT NULL',
    'current_pointer': 'id INTEGER PRIMARY KEY CHECK (id = 1), snapshot_id TEXT NOT NULL',
    'barriers': 'barrier_id TEXT PRIMARY KEY, plan_id TEXT NOT NULL, subject TEXT NOT NULL, clock TEXT NOT NULL, snapshot_seq INTEGER NOT NULL',
    'holds': 'hold_id TEXT PRIMARY KEY, reason TEXT NOT NULL, release_clock TEXT NOT NULL, state TEXT NOT NULL',
    'hold_rows': 'hold_id TEXT NOT NULL, event TEXT NOT NULL, PRIMARY KEY (hold_id, event)',
    'restricted_objects': 'object_id TEXT PRIMARY KEY, partition TEXT NOT NULL, rows INTEGER NOT NULL, state TEXT NOT NULL',
    'restricted_index': 'event TEXT PRIMARY KEY, object_id TEXT NOT NULL',
    'plans': 'plan_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, subject TEXT NOT NULL, clock TEXT NOT NULL, baseline_seq INTEGER NOT NULL, status TEXT NOT NULL, spec TEXT NOT NULL',
    'plan_steps': 'plan_id TEXT NOT NULL, seq INTEGER NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (plan_id, seq)',
    'compactions': 'token TEXT PRIMARY KEY, partition TEXT NOT NULL, baseline_seq INTEGER NOT NULL, barrier_count INTEGER NOT NULL, object_id TEXT NOT NULL, status TEXT NOT NULL',
    'audit_log': 'seq INTEGER PRIMARY KEY AUTOINCREMENT, clock TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL, detail TEXT NOT NULL',
}


class LakeError(Exception):
    """湖仓保留执行器的基础错误。"""


class BarrierError(LakeError):
    """普通读者命中的快照落后于删除屏障，必须显式拒绝。"""


class ObjectRetired(LakeError):
    """读者固定的对象已被删除计划失效或回收，不得悄悄返回旧行。"""


class BaselineConflict(LakeError):
    """压实或删除提交时基线已变化，拒绝重新引用旧对象。"""


class RestoreIncomplete(LakeError):
    """备份恢复后尚未重放删除屏障，禁止开放分析读取。"""


class AuditDenied(LakeError):
    """受限保留区只允许本地审计授权访问。"""


class Lake:
    """本地湖仓：内容寻址对象 + SQLite 目录 + 受限保留区 + 屏障日志。"""

    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / 'lake.sqlite3', timeout=5, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.execute('PRAGMA busy_timeout=5000')
        for table, columns in TABLES.items():
            self.db.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({columns})')
        self.objects = ObjectStore(self.root / 'objects')
        self.restricted = ObjectStore(self.root / 'restricted')
        defaults = {'audit_token': 'local-audit', 'restored': '0', 'barriers_applied': '1'}
        for key, value in defaults.items():
            self.db.execute('INSERT OR IGNORE INTO meta VALUES (?, ?)', (key, value))

    def close(self):
        self.db.close()

    def meta(self, key):
        row = self.db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
        return row['value'] if row else None

    def set_meta(self, key, value):
        self.db.execute('INSERT INTO meta VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, value))

    @property
    def journal_path(self):
        return self.root / 'barriers.jsonl'

    def current(self):
        row = self.db.execute(
            'SELECT s.snapshot_id, s.seq, s.manifest FROM current_pointer c JOIN snapshots s ON c.snapshot_id=s.snapshot_id WHERE c.id=1'
        ).fetchone()
        if not row:
            raise LakeError('湖仓尚未发布任何快照')
        return {'snapshot_id': row['snapshot_id'], 'seq': row['seq'], 'manifest': json.loads(row['manifest'])}

    def publish(self, kind, label, clock, manifest, connection=None):
        db = connection or self.db
        seq = (db.execute('SELECT COALESCE(MAX(seq), 0) FROM snapshots').fetchone()[0]) + 1
        snapshot_id = f'snap-{seq:06d}'
        db.execute(
            'INSERT INTO snapshots VALUES (?, ?, ?, ?, ?, ?)',
            (snapshot_id, seq, kind, label, clock, json.dumps(manifest, ensure_ascii=False, sort_keys=True)),
        )
        if kind == 'live':
            db.execute(
                'INSERT INTO current_pointer VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET snapshot_id=excluded.snapshot_id',
                (snapshot_id,),
            )
        return snapshot_id, seq

    def barrier_floor(self):
        return self.db.execute('SELECT COALESCE(MAX(snapshot_seq), 0) FROM barriers').fetchone()[0]

    def barrier_count(self):
        return self.db.execute('SELECT COUNT(*) FROM barriers').fetchone()[0]

    def insert_barrier(self, record, connection=None):
        db = connection or self.db
        db.execute(
            'INSERT OR IGNORE INTO barriers VALUES (?, ?, ?, ?, ?)',
            (record['barrier_id'], record['plan_id'], record['subject'], record['clock'], record['snapshot_seq']),
        )

    def append_journal(self, record):
        existing = set()
        if self.journal_path.exists():
            for line in self.journal_path.read_text(encoding='utf-8').splitlines():
                if line.strip():
                    existing.add(json.loads(line)['barrier_id'])
        if record['barrier_id'] in existing:
            return False
        with self.journal_path.open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + '\n')
            handle.flush()
        return True

    def live_objects(self, kind=None):
        sql = "SELECT * FROM objects WHERE state='live'"
        params = ()
        if kind:
            sql += ' AND kind=?'
            params = (kind,)
        return [dict(row) for row in self.db.execute(sql, params)]

    def manifest_from_catalog(self):
        details = {}
        for row in self.live_objects('detail'):
            details.setdefault(row['partition'], {})[row['category']] = row['object_id']
        aggregates = {}
        for row in self.live_objects('aggregate'):
            aggregates[row['partition']] = row['object_id']
        return {'details': details, 'aggregates': aggregates}

    def audit(self, clock, actor, action, detail, connection=None):
        db = connection or self.db
        db.execute(
            'INSERT INTO audit_log (clock, actor, action, detail) VALUES (?, ?, ?, ?)',
            (clock, actor, action, json.dumps(detail, ensure_ascii=False, sort_keys=True)),
        )

    def transaction(self):
        return transaction(self.db)
