"""Shared pytest fixtures.

There is no local Spark in this project: ``databricks-connect`` ships a Spark Connect client with
no JVM and no jars, so any test that needs a real engine needs the workspace. Those tests are
marked ``workspace`` and skip cleanly when no session can be established, which is what lets CI
run the pure unit tests without credentials.
"""

from __future__ import annotations

import os
import re
import uuid
from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest

from bedi_lakehouse.naming import LAYERS, Layout

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

DEFAULT_TEST_CATALOG = "beg_bedi_dev"
KEEP_SCHEMAS_ENV = "BEDI_TEST_KEEP_SCHEMAS"
RUN_ID_LENGTH = 8


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """A Databricks Connect session, or skip the test."""
    try:
        from databricks.connect import DatabricksSession

        session = DatabricksSession.builder.getOrCreate()
        session.sql("SELECT 1").collect()
    # Any failure here means "no workspace", not a bug in the code under test.
    except Exception as exc:
        pytest.skip(f"No Databricks workspace available: {type(exc).__name__}: {exc}")
    return session


def _sanitise(value: str) -> str:
    """Reduce an arbitrary string to a legal, lower-case schema name segment."""
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return cleaned or "anon"


def _drop_stale_schemas(spark: SparkSession, catalog: str, user: str) -> None:
    """Reclaim this developer's schemas from runs that died before teardown could run."""
    pattern = re.compile(rf"(?:{'|'.join(LAYERS)})_test_{re.escape(user)}_[0-9a-f]{{{RUN_ID_LENGTH}}}")
    for row in spark.sql(f"SHOW SCHEMAS IN {catalog}").collect():
        if pattern.fullmatch(row[0]):
            spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{row[0]} CASCADE")


@pytest.fixture(scope="session")
def test_catalog(spark: SparkSession) -> str:
    """The catalog fixture tests may create schemas in.

    Never inherits the session's current catalog: that would scatter tables into whatever happened
    to be default. Set ``BEDI_TEST_CATALOG`` when the documented dev catalog is not provisioned.
    """
    catalog = os.environ.get("BEDI_TEST_CATALOG", DEFAULT_TEST_CATALOG)
    existing = {row[0] for row in spark.sql("SHOW CATALOGS").collect()}
    if catalog not in existing:
        pytest.skip(f"Catalog {catalog!r} does not exist here. Set BEDI_TEST_CATALOG to one of {sorted(existing)}.")
    return catalog


@pytest.fixture(scope="session")
def layout(spark: SparkSession, test_catalog: str) -> Iterator[Layout]:
    """A private set of layer schemas, dropped when the session ends.

    The suffix carries both the developer and a per-run id, so two people — or one person running
    two branches at once — never share a schema. This is the same ``schema_suffix`` the pipeline
    uses for developer isolation, so the tests exercise the production naming path.
    """
    identity = os.environ.get("BEDI_TEST_USER") or spark.sql("SELECT current_user()").collect()[0][0]
    user = _sanitise(identity.split("@")[0])
    keep = bool(os.environ.get(KEEP_SCHEMAS_ENV))
    if not keep:
        _drop_stale_schemas(spark, test_catalog, user)

    resolved = Layout(catalog=test_catalog, schema_suffix=f"_test_{user}_{uuid.uuid4().hex[:RUN_ID_LENGTH]}")
    for layer in LAYERS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {resolved.catalog}.{resolved.schema(layer)}")
    try:
        yield resolved
    finally:
        if not keep:
            for layer in LAYERS:
                spark.sql(f"DROP SCHEMA IF EXISTS {resolved.catalog}.{resolved.schema(layer)} CASCADE")
