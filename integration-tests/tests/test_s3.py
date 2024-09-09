import pytest
import subprocess as sp
from pyspark.sql import SparkSession

from conftest import Settings, Paths


@pytest.mark.skip("Not yet working in vended credential path")
def test_create_table(spark: SparkSession):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS 
        s3.numbers_s3_parquet(as_int int, as_double double) 
        USING PARQUET
        LOCATION '{Paths.S3_TEST_PARQUET_FILE}'
    """).show()


@pytest.fixture(scope="session")
def create_parquet_if_not_exists():
    # todo: remove this workaround once table create works as expected
    assert Paths.S3_TEST_PARQUET_FILE

    create_table_cmd = """
        curl -X 'POST' \
          'http://localhost:8080/api/2.1/unity-catalog/tables' \
          -H 'accept: application/json' \
          -H 'Content-Type: application/json' \
          -d '{
          "name": "numbers_s3_parquet",
          "catalog_name": "%s",
          "schema_name": "s3",
          "table_type": "EXTERNAL",
          "data_source_format": "PARQUET",
          "columns": [
            {
              "name": "as_int",
              "type_text": "int",
              "type_json": "integer",
              "type_name": "INT",
              "position": 0,
              "nullable": true
            },
            {
              "name": "as_double",
              "type_text": "double",
              "type_json": "double",
              "type_name": "DOUBLE",
              "position": 1,
              "nullable": true
            }
          ],
          "storage_location": "%s",
          "comment": "parquet file in s3",
          "properties": {}
        }'
    """ % (Settings.CATALOG_NAME, Paths.S3_TEST_PARQUET_FILE)
    output = sp.check_output(create_table_cmd, shell=True, universal_newlines=True)
    if "error code" in output and "ALREADY EXISTS" not in output:
        raise RuntimeError(f"Couldn't create S3 test table: {output}")
    yield


def test_s3_read_vended_credentials_parquet(spark: SparkSession, create_parquet_if_not_exists):
    result = spark.sql("select * from s3.numbers_s3_parquet").toJSON().collect()
    assert result == ['{"as_int":564,"as_double":188.75535598441473}', '{"as_int":755,"as_double":883.6105633023361}',
                      '{"as_int":644,"as_double":203.4395591086936}', '{"as_int":75,"as_double":277.8802190765611}',
                      '{"as_int":42,"as_double":403.857969425109}', '{"as_int":680,"as_double":797.6912200731077}',
                      '{"as_int":821,"as_double":767.7998537403159}', '{"as_int":484,"as_double":344.00373976089304}',
                      '{"as_int":477,"as_double":380.6785614543262}', '{"as_int":131,"as_double":35.44373222835895}',
                      '{"as_int":294,"as_double":209.32243623208947}', '{"as_int":150,"as_double":329.19730274053694}',
                      '{"as_int":539,"as_double":425.66102859000944}', '{"as_int":247,"as_double":477.742227230588}',
                      '{"as_int":958,"as_double":509.3712727285101}']


def test_s3_read_vended_credentials_delta(spark: SparkSession):
    result = spark.sql("select * from s3.numbers_s3").toJSON().collect()
    assert result == ['{"as_int":564,"as_double":188.75535598441473}', '{"as_int":755,"as_double":883.6105633023361}',
                      '{"as_int":644,"as_double":203.4395591086936}', '{"as_int":75,"as_double":277.8802190765611}',
                      '{"as_int":42,"as_double":403.857969425109}', '{"as_int":680,"as_double":797.6912200731077}',
                      '{"as_int":821,"as_double":767.7998537403159}', '{"as_int":484,"as_double":344.00373976089304}',
                      '{"as_int":477,"as_double":380.6785614543262}', '{"as_int":131,"as_double":35.44373222835895}',
                      '{"as_int":294,"as_double":209.32243623208947}', '{"as_int":150,"as_double":329.19730274053694}',
                      '{"as_int":539,"as_double":425.66102859000944}', '{"as_int":247,"as_double":477.742227230588}',
                      '{"as_int":958,"as_double":509.3712727285101}']
