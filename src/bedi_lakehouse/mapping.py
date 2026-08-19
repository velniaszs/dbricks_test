"""Mapping: bronze VARIANT payload plus a mapping release becomes typed generic columns.

Every expression is a SQL string rather than a ``Column``, for the same reason as ``hashing``:
there is no local Spark to build a ``Column`` against, and a string is inspectable when a mapping
release is disputed. The generated SQL is the artefact worth reviewing.

Names only, never positions. Two source columns for one tenant and one generic column mean a
rename, and ``coalesce`` resolves each era because a payload only ever carries one of the keys.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass

from bedi_lakehouse.columns import ENTITY_KEY, TENANT_ID
from bedi_lakehouse.config import ColumnMappingRow, ConfigError, Grain
from bedi_lakehouse.hashing import quote_identifier, quote_literal

_SOURCE_COLUMN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Interpolated into `try_cast(... AS <type>)`, so it is validated rather than trusted.
_TARGET_TYPE = re.compile(r"^[A-Z]+(?:\(\s*\d+\s*(?:,\s*\d+\s*)?\))?$")

STRING_TYPE = "STRING"
TIMESTAMP_TYPE = "TIMESTAMP"
SHA_BITS = 256


@dataclass(frozen=True)
class SelectPlan:
    """The projection that turns staged bronze into silver's business columns."""

    expressions: tuple[str, ...]
    generic_columns: tuple[str, ...]


def variant_path(source_column: str) -> str:
    """Return the VARIANT path for a source column."""
    if not _SOURCE_COLUMN.fullmatch(source_column):
        raise ConfigError(
            f"Source column {source_column!r} would need bracket-notation VARIANT paths; "
            "sanitise the header at bronze before mapping it."
        )
    return f"$.{source_column}"


def source_expr(source_column: str) -> str:
    """Read one source column out of the payload as a string."""
    return f"try_variant_get(payload, '{variant_path(source_column)}', 'STRING')"


def _by_tenant(rows: Sequence[ColumnMappingRow]) -> dict[str, list[ColumnMappingRow]]:
    grouped: dict[str, list[ColumnMappingRow]] = defaultdict(list)
    for row in rows:
        grouped[row.tenant].append(row)
    return grouped


def _coalesced(rows: Sequence[ColumnMappingRow]) -> str:
    ordered = sorted(rows, key=lambda row: (row.precedence, row.source_column))
    sources = [source_expr(row.source_column) for row in ordered]
    return sources[0] if len(sources) == 1 else f"coalesce({', '.join(sources)})"


def _case(arms: dict[str, str], tenant_column: str) -> str:
    branches = " ".join(f"WHEN {quote_literal(tenant)} THEN {arms[tenant]}" for tenant in sorted(arms))
    return f"CASE {quote_identifier(tenant_column)} {branches} END"


def raw_expr(rows: Sequence[ColumnMappingRow], *, tenant_column: str = TENANT_ID) -> str:
    """Return the untyped value for one generic column, as a CASE over tenant.

    Never a blind coalesce across tenants: two tenants may use the same source column name for
    different things, so each tenant gets its own arm.
    """
    if not rows:
        raise ConfigError("Cannot build an expression from an empty mapping row set.")
    grouped = _by_tenant(rows)
    return _case({tenant: _coalesced(group) for tenant, group in grouped.items()}, tenant_column)


def _target_type(rows: Sequence[ColumnMappingRow]) -> str:
    declared = {(row.target_type or STRING_TYPE).upper() for row in rows}
    if len(declared) > 1:
        raise ConfigError(f"Generic column {rows[0].generic_column!r} declares conflicting types: {sorted(declared)}")
    target = declared.pop()
    if not _TARGET_TYPE.fullmatch(target):
        raise ConfigError(f"Unsupported target_type {target!r}.")
    return target


def _timestamp_case(rows: Sequence[ColumnMappingRow], tenant_column: str) -> str:
    """Parse formats are per tenant, so the CASE wraps the parse rather than the reverse."""
    arms = {}
    for tenant, group in _by_tenant(rows).items():
        value = _coalesced(group)
        fmt = next((row.parse_format for row in sorted(group, key=lambda r: r.precedence) if row.parse_format), None)
        arms[tenant] = f"try_to_timestamp({value}, {quote_literal(fmt)})" if fmt else f"try_cast({value} AS TIMESTAMP)"
    return _case(arms, tenant_column)


def typed_expr(rows: Sequence[ColumnMappingRow], *, tenant_column: str = TENANT_ID) -> str:
    """Return the cast value for one generic column.

    ``try_`` variants throughout: a value that will not parse becomes NULL and is caught by the
    quality rules, rather than failing the whole stream.
    """
    target = _target_type(rows)
    if target == STRING_TYPE:
        return raw_expr(rows, tenant_column=tenant_column)
    if target == TIMESTAMP_TYPE:
        return _timestamp_case(rows, tenant_column)
    return f"try_cast({raw_expr(rows, tenant_column=tenant_column)} AS {target})"


def _business_key_rows(
    rows: Sequence[ColumnMappingRow], business_key: Sequence[str]
) -> dict[str, list[ColumnMappingRow]]:
    grouped: dict[str, list[ColumnMappingRow]] = defaultdict(list)
    for row in rows:
        if row.is_business_key:
            grouped[row.generic_column].append(row)

    declared, mapped = set(business_key), set(grouped)
    if declared != mapped:
        raise ConfigError(
            f"Business key disagrees between entity and mapping: grain declares {sorted(declared)}, "
            f"mapping flags {sorted(mapped)}."
        )
    return grouped


def entity_key_expr(rows: Sequence[ColumnMappingRow], grain: Grain) -> str:
    """Hash the tenant and business key columns into a stable entity identity.

    Concatenated without ``coalesce``, so a missing key component yields a NULL ``entity_key``
    rather than one that collides with every other incomplete row. Detecting that NULL is the
    ``missing_business_key`` policy's job.
    """
    grouped = _business_key_rows(rows, grain.business_key)
    parts = [f"'|' || {quote_identifier(grain.tenant_column)}"]
    parts += [f"'|' || {raw_expr(grouped[column], tenant_column=grain.tenant_column)}" for column in grain.business_key]
    return f"sha2(concat({', '.join(parts)}), {SHA_BITS})"


def build_select(rows: Sequence[ColumnMappingRow], grain: Grain) -> SelectPlan:
    """Return the projection for one mapping release.

    Generic columns are ordered alphabetically rather than by their order in the mapping file, so
    that reordering rows in the CSV cannot silently change the silver schema.
    """
    promoted: dict[str, list[ColumnMappingRow]] = defaultdict(list)
    for row in rows:
        if row.generic_column and row.is_promoted:
            promoted[row.generic_column].append(row)

    ordered = tuple(sorted(promoted))
    expressions = [
        f"{typed_expr(promoted[column], tenant_column=grain.tenant_column)} AS {quote_identifier(column)}"
        for column in ordered
    ]
    expressions.append(f"{entity_key_expr(rows, grain)} AS {quote_identifier(ENTITY_KEY)}")
    return SelectPlan(expressions=tuple(expressions), generic_columns=ordered)
