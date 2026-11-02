"""Feldera <-> DuckLake connectors.

Three layers, usable independently:

``protocol``
    Pure-Python publication protocol (batch identity, guard row, decision
    function) with no I/O.
``catalog``
    DuckLake access: snapshots, change feed, and the guarded publication
    transaction.
``input_connector`` / ``output_connector``
    The two connectors, plus :mod:`fd_connectors.feldera` for the REST API.
"""

from .catalog import DuckLake, GuardInvariantError, HistoryExpiredError, PublicationResult
from .feldera import FelderaClient, FelderaError, OutputChunk
from .input_connector import InputConnector, InputSpec
from .journal import Checkpoint, Journal, JournalError
from .output_connector import OutputConnector, OutputSpec, PublicationRefused, StepAssembler
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
    "OutputConnector",
    "OutputSpec",
    "ProtocolError",
    "PublicationRefused",
    "PublicationResult",
    "StepAssembler",
    "acquire_ownership",
    "advance",
    "decide",
    "validate",
]

__version__ = "0.1.0"
