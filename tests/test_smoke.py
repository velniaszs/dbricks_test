"""Tests for the smoke job. No cluster required: Spark is faked."""

import bedi_lakehouse
from bedi_lakehouse import smoke


class _FakeDataFrame:
    def __init__(self, rows: list[tuple[int, str]]) -> None:
        self._rows = rows

    def show(self) -> None:
        print(f"<{len(self._rows)} rows>")

    def count(self) -> int:
        return len(self._rows)


class _FakeSpark:
    def createDataFrame(self, data: list[tuple[int, str]], schema: str) -> _FakeDataFrame:  # noqa: N802
        assert schema == smoke.SAMPLE_SCHEMA
        return _FakeDataFrame(data)


class _FakeBuilder:
    @staticmethod
    def getOrCreate() -> _FakeSpark:  # noqa: N802
        return _FakeSpark()


class _FakeSession:
    builder = _FakeBuilder


def test_build_banner_includes_version() -> None:
    assert bedi_lakehouse.__version__ in smoke.build_banner()


def test_run_returns_row_count() -> None:
    assert smoke.run(_FakeSpark()) == len(smoke.SAMPLE_ROWS)


def test_main_reports_banner_and_row_count(monkeypatch, capsys) -> None:
    monkeypatch.setattr(smoke, "DatabricksSession", _FakeSession)

    smoke.main()

    out = capsys.readouterr().out
    assert smoke.build_banner() in out
    assert f"rows: {len(smoke.SAMPLE_ROWS)}" in out
