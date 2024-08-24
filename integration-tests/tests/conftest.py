import os
import pytest
import signal
import subprocess as sp
import time
from pathlib import Path
from pyspark.sql import SparkSession


class Paths:
    INTEGRATION_TEST_DIR = Path(__file__).parents[1]
    PROJECT_ROOT_DIR = INTEGRATION_TEST_DIR.parent
    BIN_DIR = PROJECT_ROOT_DIR / "bin"
    DATA_DIR = PROJECT_ROOT_DIR / "etc" / "data"
    MANAGED_TABLES_DIR = DATA_DIR / "managed" / "unity" / "default" / "tables"
    EXTERNAL_TABLES_DIR = DATA_DIR / "external" / "unity" / "default" / "tables"


# todo: parameterize
class Settings:
    IVY_HOME = os.getenv("IVY_HOME", os.path.expanduser("~/.ivy2"))
    UC_VERSION = "0.2.0-SNAPSHOT"
    DELTA_SPARK_VERSION = "3.2.0"
    URI = "http://localhost:8080"
    TOKEN = ""
    CATALOG_NAME = "unity"


@pytest.fixture(scope="session", autouse=True)
def unitycatalog_server():
    # run the UC server from the integration test dir (regardless of where pytest was invoked from)
    # to make sure that we use the conf files from the integration test dir

    process = None
    try:
        process = sp.Popen(args=[Paths.BIN_DIR / "start-uc-server"],
                           start_new_session=True,
                           cwd=Paths.INTEGRATION_TEST_DIR)
        time.sleep(1)  # todo: better "wait for UC server to be ready" -- maybe poll for valid HEAD requests?
        yield
    finally:
        if process:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)


@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder
        .appName("uc-integration-tests")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # needed for the default `spark_catalog`
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.ivy", Settings.IVY_HOME)
        .config("spark.jars.packages", build_package_list())
        .config("spark.sql.defaultCatalog", Settings.CATALOG_NAME)
        .config(f"spark.sql.catalog.{Settings.CATALOG_NAME}", "io.unitycatalog.connectors.spark.UCSingleCatalog")
        .config(f"spark.sql.catalog.{Settings.CATALOG_NAME}.uri", Settings.URI)
        .config(f"spark.sql.catalog.{Settings.CATALOG_NAME}.token", Settings.TOKEN)
    )

    spark_session = builder.getOrCreate()
    yield spark_session

    spark_session.stop()


def build_package_list() -> str:
    return ",".join([
        f"io.unitycatalog:unitycatalog-spark:{Settings.UC_VERSION}",
        f"io.delta:delta-spark_2.12:{Settings.DELTA_SPARK_VERSION}",
        f"org.apache.hadoop:hadoop-aws:3.3.2",
    ])
