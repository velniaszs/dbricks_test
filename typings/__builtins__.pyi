# ruff: noqa
"""Type stubs for Databricks notebook built-in globals.

This file provides IDE support for Databricks-specific globals like spark, dbutils, display, etc.
These are automatically available in Databricks notebooks but need type hints for local development.
"""

from databricks.sdk.runtime import *
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import udf as U
from pyspark.sql.session import SparkSession

udf = U
spark: SparkSession
sc = spark.sparkContext
sqlContext: SQLContext
sql = sqlContext.sql
table = sqlContext.table
getArgument = dbutils.widgets.getArgument

def displayHTML(html): ...
def display(input=None, *args, **kwargs): ...
