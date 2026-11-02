"""Feldera <-> DuckLake connectors.

Two layers, usable independently:

``protocol``
    Pure-Python publication protocol (batch identity, guard row, decision
    function) with no I/O.
``catalog``
    DuckLake access: snapshots, change feed, and the guarded publication
    transaction.
"""

from .catalog import DuckLake, GuardInvariantError, HistoryExpiredError, PublicationResult
from .protocol import (
    Batch,
    Decision,
    Delta,
    Guard,
    Op,
    ProtocolError,
    acquire_ownership,
    advance,
    decide,
    validate,
)

__all__ = [
    "Batch",
    "Decision",
    "Delta",
    "DuckLake",
    "Guard",
    "GuardInvariantError",
    "HistoryExpiredError",
    "Op",
    "ProtocolError",
    "PublicationResult",
    "acquire_ownership",
    "advance",
    "decide",
    "validate",
]

__version__ = "0.1.0"
