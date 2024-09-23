from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, Row


def create_simple_mock_df(spark):
    schema = StructType([
        StructField("ID", StringType()),
        StructField("Name", StringType()),
        StructField("Age", IntegerType())
    ])

    rows = [
        Row(ID="JKF2G", Name="George", Age=27),
        Row(ID="WRC7K", Name="Paul", Age=22)
    ]

    rdd = spark.sparkContext.parallelize(rows, 2)
    return spark.createDataFrame(rdd, schema)


def easy_create_simple_mock_df(spark):
    rows = [
        ("Male", "Marco", 28),
        ("Female", "Song", 22),
        ("Male", "Marco", 28)
    ]

    return (spark.createDataFrame(rows)
            .toDF("Gender", "Name", "Age")
            .withColumn("ID", monotonically_increasing_id()))

