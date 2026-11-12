"""湖仓保留策略执行器：在不可变对象、历史快照和派生指标之间给出可核验的删除闭包。

仅使用本地合成数据，不提供法律判断。所有过期判断只采用调用方传入的时钟。
"""

from .core import (
    AuditDenied,
    BarrierError,
    BaselineConflict,
    Lake,
    LakeError,
    ObjectRetired,
    RestoreIncomplete,
)

__all__ = [
    'AuditDenied',
    'BarrierError',
    'BaselineConflict',
    'Lake',
    'LakeError',
    'ObjectRetired',
    'RestoreIncomplete',
]
