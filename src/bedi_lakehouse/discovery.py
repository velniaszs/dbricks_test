"""Discovery: what is sitting in the landing volume, and in what order it must be processed.

Ordering is the whole point. ``_ingest_seq`` is assigned from this order exactly once and never
recomputed, so a wrong order here is not a transient bug — it is baked into every silver version
boundary derived from it afterwards.

Scoped to one stream. Cross-stream order is meaningless because entities never span streams, so
two streams may be discovered and ingested concurrently.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from bedi_lakehouse.config import Landing, LoadMode

TENANT_SEGMENT = "tenant"
LOAD_MODE_SEGMENT = "load_mode"
SUPPORTED_SEGMENTS = (TENANT_SEGMENT, LOAD_MODE_SEGMENT)

_PLACEHOLDER = re.compile(r"^<([a-z_]+)>$")


class DiscoveryError(Exception):
    """Raised when the landing area does not match its declared contract."""


@dataclass(frozen=True)
class LandingEntry:
    """A file seen in the landing volume, before its path has been interpreted."""

    path: str
    modified_at: datetime


@dataclass(frozen=True)
class LandingFile:
    """A landing file with its tenant and load mode resolved from the folder contract."""

    path: str
    tenant: str
    load_mode: LoadMode
    modified_at: datetime


def parse_layout(layout: str) -> tuple[str, ...]:
    """Return the ordered placeholder names of a landing layout such as ``<tenant>/<load_mode>``."""
    segments = []
    for raw in layout.strip("/").split("/"):
        match = _PLACEHOLDER.fullmatch(raw)
        if not match or match.group(1) not in SUPPORTED_SEGMENTS:
            raise DiscoveryError(
                f"Landing layout {layout!r} has unsupported segment {raw!r}; expected {SUPPORTED_SEGMENTS}."
            )
        segments.append(match.group(1))

    missing = set(SUPPORTED_SEGMENTS) - set(segments)
    if missing or len(segments) != len(set(segments)):
        raise DiscoveryError(f"Landing layout {layout!r} must use each of {SUPPORTED_SEGMENTS} exactly once.")
    return tuple(segments)


def _relative_to(root: str, path: str) -> str:
    prefix = root.rstrip("/") + "/"
    if not path.startswith(prefix):
        raise DiscoveryError(f"{path!r} is not under the landing root {root!r}.")
    return path[len(prefix) :]


def classify(root: str, entry: LandingEntry, segments: Sequence[str]) -> LandingFile:
    """Interpret one landing path against the folder contract.

    A folder that is not a known load mode is an error rather than a guess: mis-tagging a delta as
    a full load tombstones every entity the delta omits, which is most of them.
    """
    parts = _relative_to(root, entry.path).split("/")
    if len(parts) != len(segments) + 1:
        raise DiscoveryError(f"{entry.path!r} does not match layout {'/'.join(f'<{s}>' for s in segments)}.")

    resolved = dict(zip(segments, parts, strict=False))
    try:
        load_mode = LoadMode(resolved[LOAD_MODE_SEGMENT].lower())
    except ValueError as exc:
        expected = [mode.value for mode in LoadMode]
        raise DiscoveryError(
            f"{entry.path!r}: expected a {expected} folder, got {resolved[LOAD_MODE_SEGMENT]!r}."
        ) from exc

    tenant = resolved[TENANT_SEGMENT]
    if not tenant:
        raise DiscoveryError(f"{entry.path!r} has an empty tenant segment.")
    return LandingFile(path=entry.path, tenant=tenant, load_mode=load_mode, modified_at=entry.modified_at)


def _assert_orderable(files: Sequence[LandingFile]) -> None:
    """Reject same-mtime files within a tenant.

    A tie makes the path the de-facto ordering key, which has nothing to do with delivery order —
    and ``delta`` sorts before ``full``, so the chain would be built backwards and unchanged
    resends would collapse the wrong way round. Ties across tenants are harmless.
    """
    seen: dict[tuple[str, datetime], str] = {}
    for file in files:
        key = (file.tenant, file.modified_at)
        if key in seen:
            raise DiscoveryError(
                f"{file.tenant}: {seen[key]!r} and {file.path!r} share a modification timestamp "
                f"({file.modified_at.isoformat()}). File order is the only ordering signal available."
            )
        seen[key] = file.path


def discover(root: str, entries: Iterable[LandingEntry], landing: Landing) -> tuple[LandingFile, ...]:
    """Return every landing file for a stream, in the order it must be ingested."""
    suffix = f".{landing.format.lower().lstrip('.')}"
    segments = parse_layout(landing.layout)
    files = [classify(root, entry, segments) for entry in entries if entry.path.lower().endswith(suffix)]

    _assert_orderable(files)
    # The path tie-break only ever resolves cross-tenant ties, which are arbitrary by nature.
    return tuple(sorted(files, key=lambda file: (file.modified_at, file.path)))


def pending(files: Iterable[LandingFile], already_ingested: Iterable[str]) -> tuple[LandingFile, ...]:
    """Drop files bronze has already recorded, so a re-run is a no-op rather than a duplicate."""
    seen = set(already_ingested)
    return tuple(file for file in files if file.path not in seen)


def list_volume(root: str) -> tuple[LandingEntry, ...]:
    """List a Unity Catalog volume path recursively via the Files API."""
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    found: list[LandingEntry] = []
    stack = [root.rstrip("/")]
    while stack:
        for item in client.files.list_directory_contents(stack.pop()):
            if item.is_directory:
                stack.append(item.path)
            else:
                found.append(
                    LandingEntry(path=item.path, modified_at=datetime.fromtimestamp(item.last_modified / 1000, tz=UTC))
                )
    return tuple(found)
