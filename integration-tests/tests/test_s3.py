import os

import pytest
from pyspark.sql import SparkSession

# e.g. s3://my-bucket/prefix/path/to/my/table/metadata
S3_TEST_LOCATION = os.getenv("S3_TEST_LOCATION")


@pytest.mark.skip("TODO: AWS credential chain issues w/ SSO")
def test_register_table(spark: SparkSession):
    create = spark.sql(f"""
        CREATE TABLE default.s3_table
        USING delta
        LOCATION '{S3_TEST_LOCATION}'
    """)

    # todo: the above probably doesn't work the way I want yet -- just call UC cli to register table ?

    create_result = create.collect()

    result = spark.sql("select * from default.s3_table").collect()
    assert result
