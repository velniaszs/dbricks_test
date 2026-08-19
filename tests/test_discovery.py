"""Landing discovery: the folder contract, and the ordering guarantees built on it."""

from __future__ import annotations

import io
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from bedi_lakehouse.config import Landing, LoadMode
from bedi_lakehouse.discovery import (
    DiscoveryError,
    LandingEntry,
    classify,
    discover,
    list_volume,
    parse_layout,
    pending,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from bedi_lakehouse.naming import Layout

ROOT = "/Volumes/cat/bronze/landing/aas_doors/requirement"
SEGMENTS = ("tenant", "load_mode")
LANDING = Landing(volume="landing", path="aas_doors/requirement", layout="<tenant>/<load_mode>", format="csv")


def _entry(relative: str, minute: int = 0) -> LandingEntry:
    return LandingEntry(path=f"{ROOT}/{relative}", modified_at=datetime(2026, 3, 5, 12, minute, tzinfo=UTC))


class TestParseLayout:
    def test_reads_the_documented_layout(self) -> None:
        assert parse_layout("<tenant>/<load_mode>") == ("tenant", "load_mode")

    def test_order_is_preserved(self) -> None:
        assert parse_layout("<load_mode>/<tenant>") == ("load_mode", "tenant")

    def test_rejects_an_unknown_segment(self) -> None:
        with pytest.raises(DiscoveryError, match="unsupported segment"):
            parse_layout("<tenant>/<region>/<load_mode>")

    def test_rejects_a_literal_segment(self) -> None:
        with pytest.raises(DiscoveryError, match="unsupported segment"):
            parse_layout("incoming/<tenant>/<load_mode>")

    def test_rejects_a_layout_missing_the_load_mode(self) -> None:
        with pytest.raises(DiscoveryError, match="exactly once"):
            parse_layout("<tenant>")


class TestClassify:
    def test_resolves_tenant_and_load_mode(self) -> None:
        file = classify(ROOT, _entry("FERRARI/full/doors.csv"), SEGMENTS)
        assert (file.tenant, file.load_mode) == ("FERRARI", LoadMode.FULL)

    def test_an_unknown_folder_is_an_error_not_a_guess(self) -> None:
        with pytest.raises(DiscoveryError, match="expected a"):
            classify(ROOT, _entry("FERRARI/incremental/doors.csv"), SEGMENTS)

    def test_rejects_a_file_at_the_wrong_depth(self) -> None:
        with pytest.raises(DiscoveryError, match="does not match layout"):
            classify(ROOT, _entry("FERRARI/full/2026/doors.csv"), SEGMENTS)

    def test_rejects_a_path_outside_the_root(self) -> None:
        entry = LandingEntry(path="/Volumes/cat/bronze/other/x.csv", modified_at=datetime.now(tz=UTC))
        with pytest.raises(DiscoveryError, match="not under the landing root"):
            classify(ROOT, entry, SEGMENTS)


class TestDiscover:
    def test_ignores_files_of_another_format(self) -> None:
        entries = [_entry("FERRARI/full/doors.csv"), _entry("FERRARI/full/_SUCCESS", minute=1)]
        assert [file.path.rsplit("/", 1)[-1] for file in discover(ROOT, entries, LANDING)] == ["doors.csv"]

    def test_orders_by_modification_time(self) -> None:
        entries = [_entry("FERRARI/full/b.csv", minute=5), _entry("FERRARI/delta/a.csv", minute=1)]
        assert [file.load_mode for file in discover(ROOT, entries, LANDING)] == [LoadMode.DELTA, LoadMode.FULL]

    def test_a_tie_within_one_tenant_is_fatal(self) -> None:
        entries = [_entry("FERRARI/full/a.csv"), _entry("FERRARI/delta/b.csv")]
        with pytest.raises(DiscoveryError, match="share a modification timestamp"):
            discover(ROOT, entries, LANDING)

    def test_a_tie_across_tenants_is_harmless(self) -> None:
        entries = [_entry("FERRARI/full/a.csv"), _entry("PORSCHE/full/b.csv")]
        assert len(discover(ROOT, entries, LANDING)) == 2

    def test_a_cross_tenant_tie_still_orders_deterministically(self) -> None:
        entries = [_entry("PORSCHE/full/b.csv"), _entry("FERRARI/full/a.csv")]
        assert [file.tenant for file in discover(ROOT, entries, LANDING)] == ["FERRARI", "PORSCHE"]


class TestPending:
    def test_skips_files_already_in_bronze(self) -> None:
        files = discover(ROOT, [_entry("FERRARI/full/a.csv"), _entry("FERRARI/full/b.csv", minute=1)], LANDING)
        assert [file.path for file in pending(files, {files[0].path})] == [files[1].path]

    def test_an_empty_bronze_leaves_everything_pending(self) -> None:
        files = discover(ROOT, [_entry("FERRARI/full/a.csv")], LANDING)
        assert pending(files, set()) == files


@pytest.mark.workspace
class TestListVolume:
    """The Files API listing has to agree with the folder contract on a real volume."""

    @pytest.fixture(scope="class")
    def landing_root(self, spark: SparkSession, layout: Layout) -> str:
        from databricks.sdk import WorkspaceClient

        spark.sql(f"CREATE VOLUME IF NOT EXISTS {layout.catalog}.{layout.schema('bronze')}.landing")
        root = layout.landing_path("landing", "aas_doors/requirement")
        client = WorkspaceClient()
        # One file per tenant: two files written in the same second would tie, which is fatal by design.
        for relative in ("FERRARI/full/a.csv", "PORSCHE/delta/b.csv", "ACME/full/c.csv"):
            client.files.upload(f"{root}/{relative}", io.BytesIO(b"DOOR_NO\nD1\n"), overwrite=True)
        return root

    def test_finds_every_file_recursively(self, landing_root: str) -> None:
        assert len(list_volume(landing_root)) == 3

    def test_reports_a_usable_modification_time(self, landing_root: str) -> None:
        assert all(entry.modified_at.year >= 2026 for entry in list_volume(landing_root))

    def test_discovery_classifies_what_the_api_returns(self, landing_root: str) -> None:
        files = discover(landing_root, list_volume(landing_root), LANDING)
        assert {(file.tenant, file.load_mode) for file in files} == {
            ("FERRARI", LoadMode.FULL),
            ("PORSCHE", LoadMode.DELTA),
            ("ACME", LoadMode.FULL),
        }
