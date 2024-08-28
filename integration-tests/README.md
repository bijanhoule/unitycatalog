# Running integration tests

## Prerequisites
<!-- todo: update spark tests runner as java rather than python -->
<!-- todo: poetry vs plain venv? -->
<!-- todo: use something like tox/nox for a matrix of python/pyspark versions? -->
<!-- todo: automate bucket setup and/or set up permanent shared bucket -->

- Run `build/sbt clean package publishLocal spark/publishLocal` to publish spark connector to local maven cache
- Install [poetry](https://python-poetry.org/docs/#installation)
- Run `poetry install` to initialize testing env
- Set up a testing S3 bucket and IAM role, and add those to integration test [server.properties](./etc/conf/server.properties)
  - IAM Role needs trust policy so that the identity running the UC server can assume
  - IAM role needs policy to read/write/list from s3 bucket (or at least a prefix)
  - Copy testing parquet file to your S3 bucket [d1df15d1-33d8-45ab-ad77-465476e2d5cd-000.parquet](../etc/data/external/unity/default/tables/numbers/d1df15d1-33d8-45ab-ad77-465476e2d5cd-000.parquet)

## Running tests

```shell
cd integration-tests
export S3_TEST_PARQUET_FILE=s3://my-bucket/prefix/path/to/my/d1df15d1-33d8-45ab-ad77-465476e2d5cd-000.parquet
# The UC Server will need to be able to assume your AWS IAM Role, so you may need to specify the AWS user or profile: 
export AWS_PROFILE=your-root-profile
# --or for a user identity--
# export AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/etc
poetry run pytest -vv
```
