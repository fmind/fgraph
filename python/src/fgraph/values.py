"""Canonical value encoding shared by storage, query, portable events, and CLI surfaces."""

from __future__ import annotations

import base64
import binascii
import builtins
import hashlib
import json
import math
import re
import struct
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from fgraph.errors import TooLarge
from fgraph.errors import TypeError as FGraphTypeError

BLOB_THRESHOLD = 256
MAX_VALUE_BYTES = 1_048_576
MAX_JSON_DEPTH = 64
MAX_JSON_DOCUMENT_DEPTH = 80
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
INSTANT_MIN = -62_135_596_800_000_000
INSTANT_MAX = 253_402_300_799_999_999
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
RFC3339_PATTERN = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})"
)
ATTRIBUTE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._-]*$")

REF = 0
BOOL = 1
INT = 2
FLOAT = 3
TEXT = 4
INSTANT = 5
BYTES = 6
VECTOR = 7
TEXT_REF = 8
BYTES_REF = 9
JSON = 10

TAG_NAMES = {
    REF: "ref",
    BOOL: "bool",
    INT: "int",
    FLOAT: "float",
    TEXT: "text",
    INSTANT: "instant",
    BYTES: "bytes",
    VECTOR: "vector",
    TEXT_REF: "text",
    BYTES_REF: "bytes",
    JSON: "json",
}
TYPE_NAMES = frozenset({"ref", "bool", "int", "float", "text", "instant", "bytes", "vector", "json"})


def indirect_digest(tag: int, data: bytes) -> bytes:
    """Hash indirect content with its physical tag as a one-byte domain."""
    return hashlib.sha256(bytes((tag,)) + data).digest()


@dataclass(frozen=True, slots=True)
class Encoded:
    """A logical value reduced to its normative SQLite representation."""

    tag: int
    stored: int | float | str | bytes
    logical: Any
    blob: str | bytes | None = None


@dataclass(frozen=True, slots=True)
class Cell:
    """Hashable typed value used by the in-process Datalog evaluator."""

    tag: int
    value: Any


def canonical_json(value: Any) -> str:
    """Encode JSON with fgraph's canonical key, string, and binary64 number rules."""
    return _canonical_json(value, max_depth=MAX_JSON_DEPTH)


def _canonical_json_document(value: Any) -> str:
    """Encode a validated wire envelope without charging it to a logical value's depth."""
    return _canonical_json(value, max_depth=MAX_JSON_DOCUMENT_DEPTH)


def _canonical_json(value: Any, *, max_depth: int) -> str:
    try:
        result = _canonical_part(value, depth=0, max_depth=max_depth)
    except (ValueError, builtins.TypeError) as exc:
        raise FGraphTypeError(f"invalid JSON value {value!r}; use finite JSON scalars, arrays, and objects") from exc
    return result


def _canonical_float(value: float) -> str:
    if not math.isfinite(value):
        raise ValueError("non-finite JSON number")
    if value == 0:
        return "0"
    if value.is_integer() and INT64_MIN <= int(value) <= INT64_MAX:
        # Preserve the exact integer represented by binary64 at the int64
        # boundary instead of expanding the rounded repr() mantissa.
        return str(int(value))
    rendered = repr(value).lower()
    absolute = abs(value)
    if "e" not in rendered:
        return rendered.removesuffix(".0")
    mantissa, exponent_text = rendered.split("e")
    exponent = int(exponent_text)
    integral_outside_int64 = value.is_integer()
    if 1e-6 <= absolute < 1e21 and not integral_outside_int64:
        sign = ""
        if mantissa.startswith("-"):
            sign, mantissa = "-", mantissa[1:]
        digits = mantissa.replace(".", "")
        decimal_position = 1 + exponent
        if decimal_position <= 0:
            return f"{sign}0.{('0' * -decimal_position)}{digits}"
        if decimal_position >= len(digits):
            return f"{sign}{digits}{'0' * (decimal_position - len(digits))}"
        return f"{sign}{digits[:decimal_position]}.{digits[decimal_position:]}"
    normalized_mantissa = mantissa.removesuffix(".0")
    exponent_sign = "+" if exponent >= 0 else "-"
    return f"{normalized_mantissa}e{exponent_sign}{abs(exponent)}"


def _canonical_part(value: Any, *, depth: int, max_depth: int) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        if not INT64_MIN <= value <= INT64_MAX:
            raise ValueError("JSON integer exceeds signed 64-bit range")
        return str(value)
    if isinstance(value, float):
        return _canonical_float(value)
    if isinstance(value, str):
        _utf8(value)
        return json.encoder.encode_basestring(value)
    if isinstance(value, Mapping):
        child_depth = depth + 1
        if child_depth > max_depth:
            raise TooLarge(f"JSON nesting depth exceeds {max_depth}; flatten deeply nested arrays and objects")
        if not all(isinstance(key, str) for key in value):
            raise builtins.TypeError("JSON object keys must be strings")
        for key in value:
            _utf8(key)
        return (
            "{"
            + ",".join(
                f"{json.encoder.encode_basestring(key)}:"
                f"{_canonical_part(value[key], depth=child_depth, max_depth=max_depth)}"
                for key in sorted(value)
            )
            + "}"
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        child_depth = depth + 1
        if child_depth > max_depth:
            raise TooLarge(f"JSON nesting depth exceeds {max_depth}; flatten deeply nested arrays and objects")
        return "[" + ",".join(_canonical_part(item, depth=child_depth, max_depth=max_depth) for item in value) + "]"
    raise builtins.TypeError(f"unsupported JSON value {value!r}")


def _check_size(data: bytes, value: Any) -> None:
    if len(data) > MAX_VALUE_BYTES:
        raise TooLarge(
            f"value {type(value).__name__} is {len(data)} bytes; keep fact values at or below {MAX_VALUE_BYTES} bytes"
        )


def _utf8(value: str) -> bytes:
    try:
        return value.encode()
    except UnicodeEncodeError as exc:
        raise FGraphTypeError(f"string {value!r} is not valid UTF-8; remove unpaired surrogate code points") from exc


def _instant(value: Any) -> int:
    if isinstance(value, bool):
        raise FGraphTypeError(f"invalid instant {value!r}; use integer microseconds or an RFC 3339 UTC string")
    if isinstance(value, int):
        if INSTANT_MIN <= value <= INSTANT_MAX:
            return value
        raise FGraphTypeError(
            f"instant {value!r} is outside RFC 3339 years 0001..9999; use representable UTC microseconds"
        )
    if isinstance(value, str):
        if RFC3339_PATTERN.fullmatch(value) is None:
            raise FGraphTypeError(f"invalid instant {value!r}; use RFC 3339 such as 2026-08-24T10:00:00Z")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise FGraphTypeError(f"invalid instant {value!r}; use RFC 3339 such as 2026-08-24T10:00:00Z") from exc
        if parsed.tzinfo is None:
            raise FGraphTypeError(f"instant {value!r} has no timezone; include Z or an explicit UTC offset")
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        try:
            delta = parsed.astimezone(UTC) - epoch
        except (OverflowError, ValueError) as exc:
            raise FGraphTypeError(
                f"instant {value!r} normalizes outside RFC 3339 years 0001..9999; use a representable UTC instant"
            ) from exc
        return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    raise FGraphTypeError(f"invalid instant {value!r}; use integer microseconds or an RFC 3339 UTC string")


def _bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        result = value
    elif isinstance(value, str):
        try:
            result = base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise FGraphTypeError(f"invalid bytes value {value!r}; use standard padded base64") from exc
    else:
        raise FGraphTypeError(f"invalid bytes value {value!r}; use bytes or a standard padded base64 string")
    _check_size(result, value)
    return result


def _vector(value: Any) -> tuple[tuple[float, ...], bytes]:
    if not isinstance(value, (list, tuple)) or not value:
        raise FGraphTypeError(f"invalid vector {value!r}; use a non-empty array of finite numbers")
    converted: list[float] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise FGraphTypeError(f"invalid vector element {item!r}; use only finite numbers")
        number = float(item)
        if not math.isfinite(number):
            raise FGraphTypeError(f"invalid vector element {item!r}; NaN and infinity are not supported")
        converted.append(number)
    try:
        packed = struct.pack(f"<{len(converted)}f", *converted)
    except (OverflowError, struct.error) as exc:
        raise FGraphTypeError(
            f"vector {value!r} cannot be represented as float32; reduce its element magnitudes"
        ) from exc
    _check_size(packed, value)
    # Reading the float32 bytes back makes every API observe the persisted precision.
    logical = struct.unpack(f"<{len(converted)}f", packed)
    return logical, packed


def encode(value: Any, resolve_ref: Callable[[Any], int] | None = None) -> Encoded:
    """Parse one Python/wire value into a tagged SQLite value."""
    if isinstance(value, dict):
        if len(value) != 1:
            raise FGraphTypeError(
                f"value object {value!r} is not a typed wrapper; wrap literal objects with {{'json': ...}}"
            )
        kind, inner = next(iter(value.items()))
        if kind == "ref":
            if resolve_ref is None:
                raise FGraphTypeError(f"reference {inner!r} cannot be resolved here; provide an entity id or name")
            target = resolve_ref(inner)
            return Encoded(REF, target, target)
        if kind == "instant":
            instant = _instant(inner)
            return Encoded(INSTANT, instant, instant)
        if kind == "bytes":
            data = _bytes(inner)
            if len(data) > BLOB_THRESHOLD:
                digest = indirect_digest(BYTES_REF, data)
                return Encoded(BYTES_REF, digest, data, data)
            return Encoded(BYTES, data, data)
        if kind == "vector":
            logical, packed = _vector(inner)
            digest = indirect_digest(VECTOR, packed)
            return Encoded(VECTOR, digest, logical, packed)
        if kind == "json":
            canonical = canonical_json(inner)
            _check_size(canonical.encode(), inner)
            return Encoded(JSON, canonical, canonical)
        raise FGraphTypeError(
            f"unknown typed wrapper {kind!r}; use ref, instant, bytes, vector, or json (tmp is only valid as an id)"
        )
    if value is None:
        raise FGraphTypeError("null is not a fact scalar; wrap it as {'json': null} when null is domain data")
    if isinstance(value, bool):
        return Encoded(BOOL, int(value), value)
    if isinstance(value, int):
        if not INT64_MIN <= value <= INT64_MAX:
            raise FGraphTypeError(f"integer {value!r} exceeds signed 64-bit range; store a smaller integer or text")
        return Encoded(INT, value, value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise FGraphTypeError(f"float {value!r} is not finite; NaN and infinity are not supported")
        return Encoded(FLOAT, value, value)
    if isinstance(value, str):
        data = _utf8(value)
        _check_size(data, value)
        if len(data) > BLOB_THRESHOLD:
            digest = indirect_digest(TEXT_REF, data)
            return Encoded(TEXT_REF, digest, value, value)
        return Encoded(TEXT, value, value)
    if isinstance(value, bytes):
        data = _bytes(value)
        if len(data) > BLOB_THRESHOLD:
            digest = indirect_digest(BYTES_REF, data)
            return Encoded(BYTES_REF, digest, data, data)
        return Encoded(BYTES, data, data)
    raise FGraphTypeError(f"unsupported fact value {value!r}; use a scalar or a typed wrapper")


def type_name(tag: int) -> str:
    """Return the logical schema type for a physical value tag."""
    return TAG_NAMES[tag]


def value_matches(declared: str | None, encoded: Encoded) -> bool:
    """Check a physical tag against an optional logical schema type."""
    return declared is None or type_name(encoded.tag) == declared


def instant_text(microseconds: int) -> str:
    """Render integer UTC microseconds as fixed-width RFC 3339."""
    microseconds = _instant(microseconds)
    seconds, micros = divmod(microseconds, 1_000_000)
    # Datetime arithmetic supports the full normative range on Windows too;
    # fromtimestamp delegates to a platform C runtime that may reject year 1.
    moment = _UNIX_EPOCH + timedelta(seconds=seconds)
    rendered = (
        f"{moment.year:04d}-{moment.month:02d}-{moment.day:02d}T"
        f"{moment.hour:02d}:{moment.minute:02d}:{moment.second:02d}"
    )
    return f"{rendered}.{micros:06d}Z"


def wire_value(tag: int, logical: Any, name_for_id: Callable[[int], Any]) -> Any:
    """Render a logical value in the normative JSON wire form."""
    if tag == REF:
        return {"ref": name_for_id(int(logical))}
    if tag == INSTANT:
        return {"instant": instant_text(int(logical))}
    if tag in (BYTES, BYTES_REF):
        return {"bytes": base64.b64encode(logical).decode("ascii")}
    if tag == VECTOR:
        return {"vector": list(logical)}
    if tag == JSON:
        return {"json": json.loads(logical)}
    return logical
