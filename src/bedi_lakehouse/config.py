"""Loading and validation of the declarations under ``config/``.

Three layers, all of them data rather than code:

* 1a — entity definition, ``config/sources/<source_system>/<entity>.yml``
* 1b — column mapping, ``config/mappings/<source_system>/<entity>.csv``
* 2 — environment, ``config/environments/<env>.yml``

Only :mod:`bedi_lakehouse.entrypoints.deploy_config` reads these files; every runtime job reads
``meta`` instead. Unknown keys are rejected rather than ignored, because a silently dropped typo
in a declaration produces a green run against the wrong contract.
"""

from __future__ import annotations

import csv
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml

from bedi_lakehouse.naming import Stream


class ConfigError(Exception):
    """A declaration is missing, malformed or contradicts another declaration."""


class Historisation(StrEnum):
    """How silver keeps history for an entity."""

    SCD2 = "scd2"
    CURRENT_ONLY = "current_only"
    INSERT_ONLY = "insert_only"
    APPEND = "append"
    SNAPSHOT = "snapshot"


class LoadMode(StrEnum):
    """Whether a delivery is authoritative about absence."""

    FULL = "full"
    DELTA = "delta"


class SequencingSource(StrEnum):
    """Where ``_ingest_seq`` comes from."""

    MANIFEST = "manifest"
    FILE_MTIME = "file_mtime"


class NewColumnPolicy(StrEnum):
    """What to do when a source delivers a column we have never seen."""

    ACCEPT = "accept"
    FAIL = "fail"


class MissingKeyPolicy(StrEnum):
    """What to do when a delivery omits the business key."""

    FAIL = "fail"
    QUARANTINE = "quarantine"


class OnViolation(StrEnum):
    """What to do with a row that fails a data quality rule."""

    QUARANTINE = "quarantine"
    DROP = "drop"
    FAIL = "fail"


def _check_keys(mapping: Mapping[str, Any], required: set[str], optional: set[str], context: str) -> None:
    present = set(mapping)
    unknown = present - required - optional
    if unknown:
        raise ConfigError(f"{context}: unknown key(s) {sorted(unknown)}; expected {sorted(required | optional)}")
    missing = required - present
    if missing:
        raise ConfigError(f"{context}: missing required key(s) {sorted(missing)}")


def _as_mapping(value: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigError(f"{context}: expected a mapping, got {type(value).__name__}")
    return value


def _as_str_sequence(value: Any, context: str) -> tuple[str, ...]:
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise ConfigError(f"{context}: expected a list of strings, got {type(value).__name__}")
    return tuple(str(item) for item in value)


def _as_enum(enum_type: type[StrEnum], value: Any, context: str) -> Any:
    try:
        return enum_type(str(value))
    except ValueError as exc:
        allowed = sorted(member.value for member in enum_type)
        raise ConfigError(f"{context}: {value!r} is not one of {allowed}") from exc


@dataclass(frozen=True)
class Landing:
    """Where a stream's files arrive and how they are read."""

    volume: str
    path: str
    layout: str
    format: str
    options: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Grain:
    """What identifies one entity, and which column carries the tenant."""

    business_key: tuple[str, ...]
    tenant_column: str


@dataclass(frozen=True)
class Sequencing:
    """How a total order is established within a stream."""

    source: SequencingSource
    manifest_column: str | None = None


@dataclass(frozen=True)
class SchemaEvolution:
    """How the stream reacts to a changed header."""

    new_column: NewColumnPolicy
    missing_business_key: MissingKeyPolicy


@dataclass(frozen=True)
class Quality:
    """Declarative quality expectations for the stream."""

    required: tuple[str, ...]
    on_violation: OnViolation


@dataclass(frozen=True)
class EntityDefinition:
    """Layer 1a — everything about one stream that is not a column mapping."""

    stream: Stream
    landing: Landing
    grain: Grain
    historisation: Historisation
    sequencing: Sequencing
    schema_evolution: SchemaEvolution
    quality: Quality


@dataclass(frozen=True)
class ColumnMappingRow:
    """Layer 1b — one row of ``config/mappings/<source_system>/<entity>.csv``."""

    tenant: str
    source_column: str
    generic_column: str | None
    target_type: str
    parse_format: str | None
    precedence: int
    is_business_key: bool
    is_promoted: bool


@dataclass(frozen=True)
class Environment:
    """Layer 2 — which catalog and which schema suffix a run writes to."""

    name: str
    catalog: str
    schema_suffix: str = ""


_LANDING_REQUIRED = {"volume", "path", "layout", "format"}
_LANDING_OPTIONAL = {"options"}
_GRAIN_REQUIRED = {"business_key", "tenant_column"}
_SEQUENCING_REQUIRED = {"source"}
_SEQUENCING_OPTIONAL = {"manifest_column"}
_EVOLUTION_REQUIRED = {"new_column", "missing_business_key"}
_QUALITY_REQUIRED = {"required", "on_violation"}
_ENTITY_REQUIRED = {
    "source_system",
    "entity",
    "landing",
    "grain",
    "historisation",
    "sequencing",
    "schema_evolution",
    "quality",
}
_ENVIRONMENT_REQUIRED = {"name", "catalog"}
_ENVIRONMENT_OPTIONAL = {"schema_suffix"}

MAPPING_COLUMNS = (
    "tenant",
    "source_column",
    "generic_column",
    "target_type",
    "parse_format",
    "precedence",
    "is_business_key",
    "is_promoted",
)

_TRUE_TOKENS = frozenset({"true", "t", "yes", "y", "1"})
_FALSE_TOKENS = frozenset({"false", "f", "no", "n", "0"})


def _parse_bool(value: str, context: str) -> bool:
    token = (value or "").strip().lower()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False
    raise ConfigError(f"{context}: {value!r} is not a boolean")


def _parse_landing(raw: Any, context: str) -> Landing:
    mapping = _as_mapping(raw, context)
    _check_keys(mapping, _LANDING_REQUIRED, _LANDING_OPTIONAL, context)
    options = _as_mapping(mapping.get("options", {}), f"{context}.options")
    return Landing(
        volume=str(mapping["volume"]),
        path=str(mapping["path"]),
        layout=str(mapping["layout"]),
        format=str(mapping["format"]),
        options=dict(options),
    )


def _parse_grain(raw: Any, context: str) -> Grain:
    mapping = _as_mapping(raw, context)
    _check_keys(mapping, _GRAIN_REQUIRED, set(), context)
    business_key = _as_str_sequence(mapping["business_key"], f"{context}.business_key")
    if not business_key:
        raise ConfigError(f"{context}.business_key: at least one column is required")
    return Grain(business_key=business_key, tenant_column=str(mapping["tenant_column"]))


def _parse_sequencing(raw: Any, context: str) -> Sequencing:
    mapping = _as_mapping(raw, context)
    _check_keys(mapping, _SEQUENCING_REQUIRED, _SEQUENCING_OPTIONAL, context)
    source = _as_enum(SequencingSource, mapping["source"], f"{context}.source")
    manifest_column = mapping.get("manifest_column")
    if source is SequencingSource.MANIFEST and not manifest_column:
        raise ConfigError(f"{context}: manifest sequencing requires manifest_column")
    return Sequencing(source=source, manifest_column=str(manifest_column) if manifest_column else None)


def _parse_schema_evolution(raw: Any, context: str) -> SchemaEvolution:
    mapping = _as_mapping(raw, context)
    _check_keys(mapping, _EVOLUTION_REQUIRED, set(), context)
    return SchemaEvolution(
        new_column=_as_enum(NewColumnPolicy, mapping["new_column"], f"{context}.new_column"),
        missing_business_key=_as_enum(
            MissingKeyPolicy, mapping["missing_business_key"], f"{context}.missing_business_key"
        ),
    )


def _parse_quality(raw: Any, context: str) -> Quality:
    mapping = _as_mapping(raw, context)
    _check_keys(mapping, _QUALITY_REQUIRED, set(), context)
    return Quality(
        required=_as_str_sequence(mapping["required"], f"{context}.required"),
        on_violation=_as_enum(OnViolation, mapping["on_violation"], f"{context}.on_violation"),
    )


def parse_entity_definition(raw: Any, context: str) -> EntityDefinition:
    """Validate one already-loaded layer 1a document.

    Args:
        raw: The parsed YAML document.
        context: Human-readable origin, used in error messages.

    Returns:
        The validated entity definition.

    Raises:
        ConfigError: If a key is unknown, missing or holds an unsupported value.
    """
    mapping = _as_mapping(raw, context)
    _check_keys(mapping, _ENTITY_REQUIRED, set(), context)
    try:
        stream = Stream(source_system=str(mapping["source_system"]), entity=str(mapping["entity"]))
    except ValueError as exc:
        raise ConfigError(f"{context}: {exc}") from exc
    return EntityDefinition(
        stream=stream,
        landing=_parse_landing(mapping["landing"], f"{context}.landing"),
        grain=_parse_grain(mapping["grain"], f"{context}.grain"),
        historisation=_as_enum(Historisation, mapping["historisation"], f"{context}.historisation"),
        sequencing=_parse_sequencing(mapping["sequencing"], f"{context}.sequencing"),
        schema_evolution=_parse_schema_evolution(mapping["schema_evolution"], f"{context}.schema_evolution"),
        quality=_parse_quality(mapping["quality"], f"{context}.quality"),
    )


def load_entity_definition(path: Path) -> EntityDefinition:
    """Load and validate one layer 1a YAML file."""
    return parse_entity_definition(yaml.safe_load(path.read_text(encoding="utf-8")), str(path))


def load_entity_definitions(config_root: Path) -> dict[str, EntityDefinition]:
    """Load every entity definition under ``<config_root>/sources``.

    Args:
        config_root: The ``config/`` directory, as deployed to the workspace.

    Returns:
        Definitions keyed by stream name, for example ``aas_doors__requirement``.

    Raises:
        ConfigError: If the sources directory is absent, or two files declare the same stream.
    """
    sources = config_root / "sources"
    if not sources.is_dir():
        raise ConfigError(f"{sources} does not exist")
    definitions: dict[str, EntityDefinition] = {}
    for path in sorted(sources.glob("*/*.yml")):
        definition = load_entity_definition(path)
        name = definition.stream.name
        if name in definitions:
            raise ConfigError(f"{path}: stream {name} is already declared elsewhere")
        definitions[name] = definition
    return definitions


def load_column_mapping(path: Path) -> tuple[ColumnMappingRow, ...]:
    """Load and validate one layer 1b CSV file.

    Args:
        path: The mapping CSV.

    Returns:
        One row per ``(tenant, source_column)`` declaration, in file order.

    Raises:
        ConfigError: If the header is wrong, a value is not parseable, or a
            ``(tenant, source_column)`` pair repeats.
    """
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != MAPPING_COLUMNS:
            raise ConfigError(f"{path}: header must be exactly {list(MAPPING_COLUMNS)}, got {reader.fieldnames}")
        rows: list[ColumnMappingRow] = []
        seen: set[tuple[str, str]] = set()
        for line_no, record in enumerate(reader, start=2):
            context = f"{path}:{line_no}"
            key = (record["tenant"].strip(), record["source_column"].strip())
            if key in seen:
                raise ConfigError(f"{context}: duplicate mapping for tenant {key[0]!r} column {key[1]!r}")
            seen.add(key)
            rows.append(_parse_mapping_row(record, context))
    if not rows:
        raise ConfigError(f"{path}: mapping file has no rows")
    return tuple(rows)


def _parse_mapping_row(record: Mapping[str, str], context: str) -> ColumnMappingRow:
    precedence = (record["precedence"] or "1").strip()
    if not precedence.isdigit():
        raise ConfigError(f"{context}: precedence {record['precedence']!r} is not a positive integer")
    return ColumnMappingRow(
        tenant=record["tenant"].strip(),
        source_column=record["source_column"].strip(),
        generic_column=record["generic_column"].strip() or None,
        target_type=(record["target_type"].strip() or "STRING").upper(),
        parse_format=record["parse_format"].strip() or None,
        precedence=int(precedence),
        is_business_key=_parse_bool(record["is_business_key"], f"{context}.is_business_key"),
        is_promoted=_parse_bool(record["is_promoted"], f"{context}.is_promoted"),
    )


def load_environment(config_root: Path, name: str) -> Environment:
    """Load and validate ``<config_root>/environments/<name>.yml``.

    Raises:
        ConfigError: If the file is absent, malformed, or declares a different name.
    """
    path = config_root / "environments" / f"{name}.yml"
    if not path.is_file():
        raise ConfigError(f"{path} does not exist")
    mapping = _as_mapping(yaml.safe_load(path.read_text(encoding="utf-8")), str(path))
    _check_keys(mapping, _ENVIRONMENT_REQUIRED, _ENVIRONMENT_OPTIONAL, str(path))
    declared = str(mapping["name"])
    if declared != name:
        raise ConfigError(f"{path}: declares name {declared!r} but is filed as {name!r}")
    return Environment(
        name=declared,
        catalog=str(mapping["catalog"]),
        schema_suffix=str(mapping.get("schema_suffix", "")),
    )
