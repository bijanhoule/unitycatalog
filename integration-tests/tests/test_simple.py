from pyspark.sql import SparkSession


def test_sanity(spark: SparkSession):
    assert spark.sql("select 1").first().asDict() == {'1': 1}


def test_list_schemas(spark: SparkSession):
    assert spark.sql("show schemas").first().asDict() == {'namespace': 'default'}


def test_list_tables(spark: SparkSession):
    assert spark.sql("show tables in default").toJSON().collect() == [
        '{"namespace":"default","tableName":"marksheet","isTemporary":false}',
        # todo: put this back in once i figure out uniform table bootstrapping
        # '{"namespace":"default","tableName":"marksheet_uniform","isTemporary":false}',
        '{"namespace":"default","tableName":"numbers","isTemporary":false}',
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
