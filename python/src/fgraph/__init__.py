"""fgraph: an embedded temporal fact store in one SQLite file."""

from fgraph.errors import (
    Conflict,
    FGraphError,
    FormatError,
    NotFound,
    QueryError,
    ReadOnly,
    SchemaError,
    TooLarge,
    TypeError,
    Unsupported,
)
from fgraph.models import Result, SearchResult, TxReport
from fgraph.store import Db, connect, restore_backup

__all__ = [
    "Conflict",
    "Db",
    "FGraphError",
    "FormatError",
    "NotFound",
    "QueryError",
    "ReadOnly",
    "Result",
    "SchemaError",
    "SearchResult",
    "TooLarge",
    "TxReport",
    "TypeError",
    "Unsupported",
    "connect",
    "restore_backup",
]

__version__ = "1.2.0"
