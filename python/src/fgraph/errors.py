"""Typed public errors for fgraph."""


class FGraphError(Exception):
    """Base class for every public fgraph error."""


class NotFound(FGraphError):
    """An entity, attribute, transaction, or lookup was not found."""


class Conflict(FGraphError):
    """Facts conflict with cardinality or uniqueness constraints."""


class SchemaError(FGraphError):
    """A schema declaration is invalid for the stored facts."""


class TypeError(FGraphError):  # noqa: A001
    """A value does not match its declared fgraph type."""


class QueryError(FGraphError):
    """A query is malformed or unsupported."""


class FormatError(FGraphError):
    """A SQLite file conflicts with the fgraph format."""


class ReadOnly(FGraphError):
    """A write was attempted through a read-only surface."""


class TooLarge(FGraphError):
    """A value exceeds the one-megabyte fact value boundary."""


class Unsupported(FGraphError):
    """A valid operation is deliberately outside the API v1 contract."""
