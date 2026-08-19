"""Row and schema fingerprints. Pure, catalog-free and dependency-free.

Nothing here may import :mod:`bedi_lakehouse.config`, :mod:`bedi_lakehouse.naming` or anything
that reads ``meta``. The rebuild guarantee depends on ``_row_hash`` being a function of source
values alone, so that a version boundary can never move because our modelling changed.

Every function returns a SQL expression string rather than a Spark ``Column``, which makes the
whole module unit-testable without a Spark session and keeps the generated SQL inspectable when a
customer disputes a version boundary.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable

# Values that sources use to mean "absent". Normalised to NULL before hashing so that a project
# switching from "N/A" to an empty string does not mint a version.
NULL_TOKENS = ("", "NULL", "N/A", "NA", "-", "#N/A", "(NULL)", "NONE")

# Hash recorded instead of a row fingerprint on a tombstone version.
DELETED_HASH = "__deleted__"

SHA_BITS = 256

# V1 hashed absent values as a literal NUL character. A NUL cannot be written portably inside a
# SQL string literal, so it is reconstructed from bytes instead. Changing this changes every
# _row_hash ever computed and invalidates the V1 reconciliation baseline.
NULL_SENTINEL_SQL = "decode(unhex('00'), 'UTF-8')"


def quote_identifier(name: str) -> str:
    """Backtick-quote a column name, doubling any embedded backtick."""
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def quote_literal(value: str) -> str:
    """Single-quote a string literal, escaping backslashes and quotes."""
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def norm_expr(column: str) -> str:
    """Return the normalisation expression used for hashing only.

    The stored payload always keeps the raw value; normalisation exists so that whitespace and
    null-token churn do not look like data changes.

    Args:
        column: Source column name, as delivered.

    Returns:
        A SQL expression yielding the trimmed value, or NULL for any recognised null token.
    """
    value = f"trim({quote_identifier(column)})"
    tokens = ", ".join(quote_literal(token) for token in NULL_TOKENS)
    return f"CASE WHEN upper({value}) IN ({tokens}) THEN NULL ELSE {value} END"


def canon_expr(columns: Iterable[str]) -> str:
    """Return the canonical JSON expression for a set of source columns.

    Keys are sorted, so reordering columns in the source cannot change the fingerprint.

    Args:
        columns: Source column names participating in the fingerprint.

    Returns:
        A SQL expression yielding the canonical JSON string.

    Raises:
        ValueError: If no columns are supplied.
    """
    ordered = sorted(columns)
    if not ordered:
        raise ValueError("Cannot canonicalise an empty column set.")
    fields = ", ".join(
        f"coalesce({norm_expr(column)}, {NULL_SENTINEL_SQL}) AS {quote_identifier(column)}" for column in ordered
    )
    return f"to_json(struct({fields}))"


def row_hash_expr(columns: Iterable[str]) -> str:
    """Return the ``_row_hash`` expression: one hash over all normalised source values.

    Catalog-free by construction — it takes source column names and nothing else.
    """
    return f"sha2({canon_expr(columns)}, {SHA_BITS})"


def schema_version(columns: Iterable[str]) -> str:
    """Return the ``_schema_ver`` fingerprint of a delivered header.

    Order-insensitive, so a source reordering its columns does not register as schema drift.
    """
    ordered = sorted(columns)
    if not ordered:
        raise ValueError("Cannot fingerprint an empty header.")
    return hashlib.sha256("|".join(ordered).encode("utf-8")).hexdigest()
