import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def build_orders_stream(spark: SparkSession):
    schema = StructType(
        [
            StructField("customer", StringType(), True),
            StructField("product", StringType(), True),
            StructField("price", IntegerType(), True),
        ]
    )
    # helper removed — using top-level stream below per workshop starter
spark = SparkSession.builder.appName("KafkaSparkOrdersWorkshop").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("customer", StringType(), True),
        StructField("product", StringType(), True),
        StructField("price", IntegerType(), True),
    ]
)

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .load()
)

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_value")

orders_df = json_df.select(from_json(col("json_value"), schema).alias("data")).select(
    "data.*"
)

# Task code will be written below this line

query = orders_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()