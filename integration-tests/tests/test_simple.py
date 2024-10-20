from pyspark.sql import SparkSession


def test_sanity(spark: SparkSession):
    assert spark.sql("select 1").first().asDict() == {'1': 1}


def test_list_schemas(spark: SparkSession):
    assert spark.sql("show schemas").toJSON().collect() == ['{"namespace":"default"}', '{"namespace":"s3"}']


def test_list_tables(spark: SparkSession):
    assert spark.sql("show tables in default").toJSON().collect() == [
        '{"namespace":"default","tableName":"marksheet","isTemporary":false}',
        '{"namespace":"default","tableName":"marksheet_uniform","isTemporary":false}',
        '{"namespace":"default","tableName":"numbers","isTemporary":false}',
        '{"namespace":"default","tableName":"numbers_parquet","isTemporary":false}',
        '{"namespace":"default","tableName":"user_countries","isTemporary":false}'
    ]


def test_read_table(spark: SparkSession):
    result = spark.sql("select * from default.marksheet").toJSON().collect()
    assert result == ['{"id":1,"name":"nWYHawtqUw","marks":930}', '{"id":2,"name":"uvOzzthsLV","marks":166}',
                      '{"id":3,"name":"WIAehuXWkv","marks":170}', '{"id":4,"name":"wYCSvnJKTo","marks":709}',
                      '{"id":5,"name":"VsslXsUIDZ","marks":993}', '{"id":6,"name":"ZLsACYYTFy","marks":813}',
                      '{"id":7,"name":"BtDDvLeBpK","marks":52}', '{"id":8,"name":"YISVtrPfGr","marks":8}',
                      '{"id":9,"name":"PBPJHDFjjC","marks":45}', '{"id":10,"name":"qbDuUJzJMO","marks":756}',
                      '{"id":11,"name":"EjqqWoaLJn","marks":712}', '{"id":12,"name":"jpZLMdKXpn","marks":847}',
                      '{"id":13,"name":"acpjQXpJCp","marks":649}', '{"id":14,"name":"nOKqHhRwao","marks":133}',
                      '{"id":15,"name":"kxUUZEUoKv","marks":398}']


def test_read_parquet(spark: SparkSession):
    result = spark.sql("select * from spark_catalog.default.numbers_parquet").toJSON().collect()
    assert result == ['{"as_int":564,"as_double":188.75535598441473}',
                      '{"as_int":755,"as_double":883.6105633023361}',
                      '{"as_int":644,"as_double":203.4395591086936}',
                      '{"as_int":75,"as_double":277.8802190765611}',
                      '{"as_int":42,"as_double":403.857969425109}',
                      '{"as_int":680,"as_double":797.6912200731077}',
                      '{"as_int":821,"as_double":767.7998537403159}',
                      '{"as_int":484,"as_double":344.00373976089304}',
                      '{"as_int":477,"as_double":380.6785614543262}',
                      '{"as_int":131,"as_double":35.44373222835895}',
                      '{"as_int":294,"as_double":209.32243623208947}',
                      '{"as_int":150,"as_double":329.19730274053694}',
                      '{"as_int":539,"as_double":425.66102859000944}',
                      '{"as_int":247,"as_double":477.742227230588}',
                      '{"as_int":958,"as_double":509.3712727285101}']
