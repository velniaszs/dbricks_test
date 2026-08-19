"""Fully qualified Unity Catalog name construction.

This is the only module in the package that builds a table name. Catalog, schema, the
per-developer schema suffix and the ``source_system__entity`` convention are assembled here so
that nothing else needs to know they exist. A three-part name written as a string literal
anywhere else in the codebase is a defect.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
META = "meta"

LAYERS = (BRONZE, SILVER, GOLD, META)

STREAM_SEPARATOR = "__"

_STREAM_PARTS = 2

# Lower snake case, no leading digit, no trailing underscore and no doubled underscore, so that
# joining two identifiers with STREAM_SEPARATOR is always reversible.
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")
_SUFFIX = re.compile(r"^(?:_[a-z0-9]+)*$")


def _require_identifier(value: str, label: str) -> None:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(
            f"{label} must be lower snake case without doubled underscores, got {value!r}. "
            f"{STREAM_SEPARATOR!r} is reserved as the stream separator."
        )


@dataclass(frozen=True)
class Stream:
    """A ``(source_system, entity)`` pair — the unit of ingestion, ordering and parallelism."""

    source_system: str
    entity: str

    def __post_init__(self) -> None:
        _require_identifier(self.source_system, "source_system")
        _require_identifier(self.entity, "entity")

    @classmethod
    def parse(cls, value: str) -> Stream:
        """Parse the ``source_system__entity`` form used as a job parameter.

        Args:
            value: Stream identifier, for example ``aas_doors__requirement``.

        Returns:
            The parsed stream.

        Raises:
            ValueError: If the value does not contain exactly one separator, or either half is
                not a valid identifier.
        """
        parts = value.split(STREAM_SEPARATOR)
        if len(parts) != _STREAM_PARTS:
            raise ValueError(
                f"Stream {value!r} must contain exactly one {STREAM_SEPARATOR!r}, as in 'aas_doors__requirement'."
            )
        return cls(source_system=parts[0], entity=parts[1])

    @property
    def name(self) -> str:
        """The table name for this stream, identical in every layer and every environment."""
        return f"{self.source_system}{STREAM_SEPARATOR}{self.entity}"

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True)
class Layout:
    """Resolves layer names to fully qualified names for one catalog and one schema suffix.

    The suffix isolates developers sharing the dev catalog. It is empty in test and production,
    so the same code produces ``beg_bedi_dev.silver_abaubinas.x`` and ``beg_bedi_prod.silver.x``.
    """

    catalog: str
    schema_suffix: str = ""

    def __post_init__(self) -> None:
        _require_identifier(self.catalog, "catalog")
        suffix = self.schema_suffix.strip()
        if suffix and not suffix.startswith("_"):
            suffix = f"_{suffix}"
        if not _SUFFIX.fullmatch(suffix):
            raise ValueError(f"schema_suffix must be empty or underscore-delimited, got {self.schema_suffix!r}")
        object.__setattr__(self, "schema_suffix", suffix)

    def schema(self, layer: str) -> str:
        """Return the suffixed schema name for a layer."""
        if layer not in LAYERS:
            raise ValueError(f"Unknown layer {layer!r}, expected one of {LAYERS}")
        return f"{layer}{self.schema_suffix}"

    def qualify(self, layer: str, table: str) -> str:
        """Return the three-part name for a table in a layer."""
        return f"{self.catalog}.{self.schema(layer)}.{table}"

    def bronze(self, stream: Stream) -> str:
        """One bronze table per stream."""
        return self.qualify(BRONZE, stream.name)

    def silver(self, stream: Stream) -> str:
        """One silver timeline per stream."""
        return self.qualify(SILVER, stream.name)

    def gold(self, table: str) -> str:
        """A consumer-facing gold view or table."""
        return self.qualify(GOLD, table)

    def meta(self, table: str) -> str:
        """A framework state table."""
        return self.qualify(META, table)

    def volume(self, name: str) -> str:
        """Return the ``/Volumes`` path of a landing volume in the bronze schema."""
        return f"/Volumes/{self.catalog}/{self.schema(BRONZE)}/{name}"

    def landing_path(self, volume: str, path: str) -> str:
        """Return the landing root for one stream inside a volume."""
        return f"{self.volume(volume)}/{path.strip('/')}"
