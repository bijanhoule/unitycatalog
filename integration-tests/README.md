# Running integration tests

## Prerequisites

<!-- todo: poetry vs plain venv? -->
<!-- todo: use something like tox/nox for a matrix of python/pyspark versions? -->

```shell
build/sbt clean package publishLocal spark/publishLocal
```
