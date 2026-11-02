"""JSON codecs for moving rows between DuckLake and Feldera.

Feldera's JSON input/output format gives us no schema on the wire, so a bridge
decides how each SQL type is represented.  Values are only sent when the
representation is *injective* (``decode(encode(v))`` is the original value), so
a round trip through the connector cannot silently change data.  Types that
have no exact JSON representation are refused with a :class:`CodecError` unless
the caller explicitly accepts the loss.
"""

from __future__ import annotations

import datetime as dt
import math
from collections.abc import Iterable, Mapping
from decimal import Decimal
from typing import Any


class CodecError(Exception):
    """A value cannot be represented exactly over Feldera's JSON format."""


_UNSUPPORTED_NAMES = (
    "BYTEA",
    "BLOB",
    "BIT",
    "VARBIT",
    "INTERVAL",
    "UNION",
    "STRUCT",
    "MAP",
    "LIST",
    "ARRAY",
)


def to_json(value: Any, *, allow_lossy_decimal: bool = False) -> Any:
    """Encode a DuckLake/Python value into a JSON-compatible value."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CodecError(
                f"{value!r} has no JSON representation; Feldera has no NaN or infinity literal"
            )
        return value
    if isinstance(value, Decimal):
        # Only send a decimal when the JSON number decodes back to it exactly.
        as_float = float(value)
        if Decimal(repr(as_float)) == value:
            return as_float
        if allow_lossy_decimal:
            return as_float
        raise CodecError(
            f"decimal {value} cannot be represented exactly as a JSON number "
            "(pass allow_lossy_decimal to accept the precision loss)"
        )
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None and value.utcoffset() is not None:
            raise CodecError(
                f"timestamptz {value.isoformat()} has an offset; Feldera's JSON format "
                "has no zone marker, so the zone would be lost"
            )
        return value.isoformat(sep=" ")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, dt.time):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        raise CodecError(f"interval {value!r} is not supported")
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise CodecError("binary values are not supported")
    if isinstance(value, (list, tuple)):
        return [to_json(item, allow_lossy_decimal=allow_lossy_decimal) for item in value]
    if isinstance(value, dict):
        return {
            str(k): to_json(v, allow_lossy_decimal=allow_lossy_decimal) for k, v in value.items()
        }
    raise CodecError(f"unsupported value of type {type(value).__name__}: {value!r}")


def from_json(value: Any, *, sql_type: str | None = None) -> Any:
    """Decode a JSON value back into a value that DuckDB accepts.

    ``sql_type`` is optional: when the caller knows the target column type, it
    is used to reconstruct decimals (whose scale is part of the type) and to
    reject values that would silently change type.  Without it, JSON numbers
    are handed over as Python types and DuckDB performs its own casts.
    """
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CodecError(f"{value!r} is not a finite JSON number")
        return value
    if isinstance(value, list):
        return [from_json(item, sql_type=None) for item in value]
    if isinstance(value, dict):
        return {key: from_json(item, sql_type=None) for key, item in value.items()}
    raise CodecError(f"unsupported JSON value of type {type(value).__name__}: {value!r}")


def to_json_row(row: Mapping[str, Any], *, allow_lossy_decimal: bool = False) -> dict[str, Any]:
    """Encode a whole row, refusing values that have no exact JSON form."""
    return {
        str(key): to_json(value, allow_lossy_decimal=allow_lossy_decimal)
        for key, value in row.items()
    }


def encode_delta(delta: Any, *, allow_lossy_decimal: bool = False) -> dict[str, Any]:
    """Encode a delta into a Feldera update object, e.g. ``{"insert": {...}}``."""
    return {delta.op.value: to_json_row(delta.row, allow_lossy_decimal=allow_lossy_decimal)}


def decode_update(
    update: Mapping[str, Any], *, schema: dict[str, str] | None = None
) -> dict[str, Any]:
    """Decode one Feldera update object, keeping its insert/delete tag."""
    ((op_name, payload),) = update.items()
    if isinstance(payload, Mapping) and "table" in payload and "row" in payload:
        row = payload["row"]
        decoded = {key: from_json(value) for key, value in dict(row).items()}
        return {op_name: {"table": str(payload["table"]), "row": decoded}}
    if isinstance(payload, Mapping):
        return {op_name: {key: from_json(value) for key, value in payload.items()}}
    raise CodecError(f"update payload is not an object: {payload!r}")


def decode_row(
    row: Iterable[tuple[str, Any]], *, schema: dict[str, str] | None = None
) -> dict[str, Any]:
    """Decode one Feldera output row, optionally guided by the target schema."""
    schema = schema or {}
    return {name: from_json(value, sql_type=schema.get(name)) for name, value in row}


def check_round_trip(value: Any) -> Any:
    """Encode and decode ``value``, returning the decoded result."""
    return from_json(to_json(value))


def reject_unsupported_column_types(column_types: Iterable[tuple[str, str]]) -> None:
    """Fail early for column types this bridge cannot carry losslessly."""
    bad = [
        f"{name} {sql_type}"
        for name, sql_type in column_types
        if any(token in sql_type.upper() for token in _UNSUPPORTED_NAMES)
    ]
    if bad:
        raise CodecError(
            "columns with no lossless JSON representation in Feldera's format: " + ", ".join(bad)
        )
