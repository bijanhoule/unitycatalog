import os

import pytest
from pyspark.sql import SparkSession

# e.g. s3://my-bucket/prefix/path/to/my/table/metadata
S3_TEST_LOCATION = os.getenv("S3_TEST_LOCATION")


@pytest.mark.skip("Not yet working in vended credential path")
def test_create_table(spark: SparkSession):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS 
        s3.numbers_s3_parquet(as_int int, as_double double) 
        USING PARQUET
        LOCATION '{S3_TEST_LOCATION}'
    """).show()


def test_s3_read_vended_credentials(spark: SparkSession):
    result = spark.sql("select * from s3.numbers_s3_parquet").toJSON().collect()
    assert result == ['{"as_int":564,"as_double":188.75535598441473}', '{"as_int":755,"as_double":883.6105633023361}',
                      '{"as_int":644,"as_double":203.4395591086936}', '{"as_int":75,"as_double":277.8802190765611}',
                      '{"as_int":42,"as_double":403.857969425109}', '{"as_int":680,"as_double":797.6912200731077}',
                      '{"as_int":821,"as_double":767.7998537403159}', '{"as_int":484,"as_double":344.00373976089304}',
                      '{"as_int":477,"as_double":380.6785614543262}', '{"as_int":131,"as_double":35.44373222835895}',
                      '{"as_int":294,"as_double":209.32243623208947}', '{"as_int":150,"as_double":329.19730274053694}',
                      '{"as_int":539,"as_double":425.66102859000944}', '{"as_int":247,"as_double":477.742227230588}',
                      '{"as_int":958,"as_double":509.3712727285101}']
