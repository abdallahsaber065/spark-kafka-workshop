# pyright: reportMissingImports=false

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "orders"
DEFAULT_APP_NAME = "KafkaSparkOrdersWorkshop"
WORKSHOP_TASK = "incoming"
# Change WORKSHOP_TASK to one of:
# incoming, products, expensive, discounted, count
CHECKPOINT_DIR = ".checkpoints"


def build_spark_session():
    spark = SparkSession.builder.appName(DEFAULT_APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_orders_df(spark: SparkSession):
    schema = StructType(
        [
            StructField("customer", StringType(), True),
            StructField("product", StringType(), True),
            StructField("price", IntegerType(), True),
        ]
    )

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", DEFAULT_BOOTSTRAP_SERVERS)
        .option("subscribe", DEFAULT_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    json_df = raw_df.selectExpr("CAST(value AS STRING) AS json_value")
    return json_df.select(from_json(col("json_value"), schema).alias("data")).select(
        "data.*"
    )


def build_task_df(orders_df, task: str):
    if task == "incoming":
        return orders_df
    if task == "products":
        return orders_df.select("product")
    if task == "expensive":
        return orders_df.filter(col("price") > 500)
    if task == "discounted":
        return orders_df.withColumn("discounted_price", col("price") * 0.9)
    if task == "count":
        return orders_df.groupBy("customer").count()

    raise ValueError(f"Unsupported task: {task}")


def main():
    spark = build_spark_session()
    orders_df = build_orders_df(spark)
    task_df = build_task_df(orders_df, WORKSHOP_TASK)

    output_mode = "complete" if WORKSHOP_TASK == "count" else "append"
    checkpoint_location = os.path.join(CHECKPOINT_DIR, WORKSHOP_TASK)

    query = (
        task_df.writeStream.outputMode(output_mode)
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )

    print(f"Running workshop task: {WORKSHOP_TASK}")
    print(f"Reading from Kafka topic '{DEFAULT_TOPIC}' at {DEFAULT_BOOTSTRAP_SERVERS}")
    query.awaitTermination()


if __name__ == "__main__":
    main()