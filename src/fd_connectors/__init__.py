"""Feldera <-> DuckLake connectors.

``protocol``
    Pure-Python publication protocol (batch identity, guard row, decision
    function) with no I/O.
``catalog``
    DuckLake access: snapshots, change feed, and the guarded publication
    transaction.
``feldera``
    REST client for the Feldera pipeline-manager.
``input_connector``
    The DuckLake -> Feldera connector, plus :mod:`fd_connectors.journal`.
"""

from .catalog import DuckLake, GuardInvariantError, HistoryExpiredError, PublicationResult
from .feldera import FelderaClient, FelderaError, OutputChunk
from .input_connector import InputConnector, InputSpec
from .journal import Checkpoint, Journal, JournalError
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
    "Checkpoint",
    "Decision",
    "Delta",
    "DuckLake",
    "FelderaClient",
    "FelderaError",
    "Guard",
    "GuardInvariantError",
    "HistoryExpiredError",
    "InputConnector",
    "InputSpec",
    "Journal",
    "JournalError",
    "Op",
    "OutputChunk",
    "ProtocolError",
    "PublicationResult",
    "acquire_ownership",
    "advance",
    "decide",
    "validate",
]

__version__ = "0.1.0"
