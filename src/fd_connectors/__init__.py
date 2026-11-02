"""Pure-Python publication protocol for Feldera <-> DuckLake connectors.

``protocol``
    Batch identity, the guard row, and the decision function.  No I/O.
"""

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
    "Guard",
    "Op",
    "ProtocolError",
    "acquire_ownership",
    "advance",
    "decide",
    "validate",
]

__version__ = "0.1.0"
