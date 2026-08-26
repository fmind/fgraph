"""Value encoding and canonical rendering tests."""

from __future__ import annotations

import base64
import math

import pytest

from fgraph.errors import TooLarge
from fgraph.errors import TypeError as FGraphTypeError
from fgraph.values import (
    BOOL,
    BYTES,
    BYTES_REF,
    FLOAT,
    INSTANT,
    INT,
    JSON,
    REF,
    TEXT,
    TEXT_REF,
    VECTOR,
    canonical_json,
    encode,
    instant_text,
    type_name,
    value_matches,
    wire_value,
)


def test_canonical_json_cross_runtime_numbers_and_unicode() -> None:
    value = {
        "z": {"b": 1.0, "a": -0.0},
        "numbers": [1e-7, 1e-6, 1e20, 1e21, 1.25e3],
        "unicode": "hé\u2028\u2029\n",
    }
    assert canonical_json(value) == (
        '{"numbers":[1e-7,0.000001,1e+20,1e+21,1250],"unicode":"hé\u2028\u2029\\n","z":{"a":0,"b":1}}'
    )
    assert canonical_json([float(-(2**63)), float(2**60)]) == ("[-9223372036854775808,1152921504606846976]")


@pytest.mark.parametrize(
    ("value", "tag", "logical"),
    [
        (True, BOOL, True),
        (42, INT, 42),
        (1.5, FLOAT, 1.5),
        ("text", TEXT, "text"),
        (b"raw", BYTES, b"raw"),
        ({"instant": "2026-08-24T10:00:00Z"}, INSTANT, 1_787_565_600_000_000),
        ({"bytes": "aGVsbG8="}, BYTES, b"hello"),
        ({"json": {"b": 2, "a": 1.0}}, JSON, '{"a":1,"b":2}'),
    ],
)
def test_encode_direct_values(value, tag, logical) -> None:
    encoded = encode(value)
    assert encoded.tag == tag
    assert encoded.logical == logical


def test_encode_indirection_vector_and_ref() -> None:
    long_text = "é" * 129
    text = encode(long_text)
    assert text.tag == TEXT_REF
    assert text.blob == long_text
    long_bytes = encode(b"x" * 257)
    assert long_bytes.tag == BYTES_REF
    assert long_bytes.blob == b"x" * 257
    vector = encode({"vector": [0.1, -0.2]})
    assert vector.tag == VECTOR
    assert vector.blob is not None
    assert vector.logical == pytest.approx((0.1, -0.2))
    reference = encode({"ref": "ada"}, lambda value: 65 if value == "ada" else 0)
    assert reference.tag == REF
    assert reference.stored == 65


def test_wire_rendering_and_type_groups() -> None:
    def name(entity: int) -> str | int:
        return "ada" if entity == 65 else entity

    assert wire_value(REF, 65, name) == {"ref": "ada"}
    assert wire_value(INSTANT, 0, name) == {"instant": "1970-01-01T00:00:00.000000Z"}
    assert wire_value(BYTES, b"hello", name) == {"bytes": "aGVsbG8="}
    assert wire_value(VECTOR, (1.0, 2.0), name) == {"vector": [1.0, 2.0]}
    assert wire_value(JSON, '{"x":1}', name) == {"json": {"x": 1}}
    assert wire_value(TEXT, "plain", name) == "plain"
    assert type_name(TEXT_REF) == "text"
    assert value_matches("text", encode("x"))
    assert not value_matches("int", encode("x"))
    assert instant_text(-1) == "1969-12-31T23:59:59.999999Z"


@pytest.mark.parametrize(
    "value",
    [
        None,
        [],
        {},
        {"unknown": 1},
        {"instant": True},
        {"instant": "bad"},
        {"instant": "2026-01-01T00:00:00"},
        {"instant": "0001-01-01T00:00:00+23:59"},
        {"instant": "9999-12-31T23:59:59-23:59"},
        {"bytes": "!"},
        {"vector": []},
        {"vector": [True]},
        {"vector": [math.inf]},
        math.nan,
        2**63,
    ],
)
def test_invalid_values_are_typed(value) -> None:
    with pytest.raises(FGraphTypeError):
        encode(value)


def test_too_large_values() -> None:
    with pytest.raises(TooLarge):
        encode("x" * 1_048_577)
    with pytest.raises(TooLarge):
        encode({"bytes": base64.b64encode(b"x" * 1_048_577).decode()})
    with pytest.raises(TooLarge):
        encode({"json": "x" * 1_048_577})
    assert len(canonical_json(["x" * 1_048_577])) > 1_048_576


def test_invalid_json_shape_and_ref_context() -> None:
    with pytest.raises(FGraphTypeError):
        canonical_json({1: "bad"})
    with pytest.raises(FGraphTypeError):
        canonical_json(float("inf"))
    with pytest.raises(FGraphTypeError):
        encode({"ref": "ada"})
    with pytest.raises(FGraphTypeError):
        encode({"x": 1, "y": 2})


def test_json_integer_and_rfc3339_instant_boundaries() -> None:
    assert canonical_json([-(2**63), 2**63 - 1]) == "[-9223372036854775808,9223372036854775807]"
    with pytest.raises(FGraphTypeError):
        canonical_json(2**63)
    with pytest.raises(FGraphTypeError):
        canonical_json({"nested": -(2**63) - 1})
    assert instant_text(-62_135_596_800_000_000) == "0001-01-01T00:00:00.000000Z"
    assert instant_text(253_402_300_799_999_999) == "9999-12-31T23:59:59.999999Z"
    with pytest.raises(FGraphTypeError):
        instant_text(-62_135_596_800_000_001)
    with pytest.raises(FGraphTypeError):
        instant_text(253_402_300_800_000_000)


@pytest.mark.parametrize(
    "value",
    ["\ud800", "ok\udfff"],
)
def test_unpaired_surrogates_are_typed(value: str) -> None:
    with pytest.raises(FGraphTypeError, match="UTF-8"):
        encode(value)
    with pytest.raises(FGraphTypeError, match="UTF-8"):
        canonical_json({"nested": value})
    with pytest.raises(FGraphTypeError, match="UTF-8"):
        canonical_json({value: "key"})
