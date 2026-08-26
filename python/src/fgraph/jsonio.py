"""Strict JSON input helpers for deterministic public boundaries."""

from __future__ import annotations

import json
from typing import Any

from fgraph.errors import TooLarge
from fgraph.errors import TypeError as FGraphTypeError
from fgraph.values import MAX_JSON_DOCUMENT_DEPTH


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise FGraphTypeError(f"duplicate JSON key {key!r}; keep one value so transaction intent is unambiguous")
        result[key] = value
    return result


def loads(value: str, *, context: str = "input") -> Any:
    """Decode JSON while rejecting duplicate object keys and non-finite numbers."""

    def invalid_constant(constant: str) -> None:
        raise FGraphTypeError(f"{context} contains non-finite number {constant}; use a finite JSON number")

    try:
        decoded = json.loads(value, object_pairs_hook=_object, parse_constant=invalid_constant)
    except json.JSONDecodeError as exc:
        raise FGraphTypeError(
            f"{context} is not valid JSON ({exc.msg} at column {exc.colno}); correct the JSON syntax"
        ) from exc
    except RecursionError as exc:
        raise TooLarge(
            f"{context} nesting depth exceeds {MAX_JSON_DOCUMENT_DEPTH}; flatten deeply nested arrays and objects"
        ) from exc
    _check_depth(decoded, context=context)
    return decoded


def _check_depth(value: Any, *, context: str) -> None:
    pending: list[tuple[Any, int]] = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if isinstance(item, dict):
            child_depth = depth + 1
            if child_depth > MAX_JSON_DOCUMENT_DEPTH:
                raise TooLarge(
                    f"{context} nesting depth exceeds {MAX_JSON_DOCUMENT_DEPTH}; flatten deeply nested arrays and objects"
                )
            pending.extend((child, child_depth) for child in item.values())
        elif isinstance(item, list):
            child_depth = depth + 1
            if child_depth > MAX_JSON_DOCUMENT_DEPTH:
                raise TooLarge(
                    f"{context} nesting depth exceeds {MAX_JSON_DOCUMENT_DEPTH}; flatten deeply nested arrays and objects"
                )
            pending.extend((child, child_depth) for child in item)
