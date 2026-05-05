# Script Walkthrough

This note explains the two main scripts in the workshop line by line at a high level:

- `producer_orders.py`
- `spark_kafka_consumer.py`

The goal is to help the student team explain what each part does without having to reread the raw code during discussion.

## `producer_orders.py`

### Imports

```python
from kafka import KafkaProducer
import json
import time
```

- `KafkaProducer` is the client that sends messages into Kafka.
- `json` converts Python dictionaries into JSON text.
- `time` is used to pause between messages so the stream is easier to watch live.

### Producer setup

```python
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)
```

- `bootstrap_servers="localhost:9092"` tells the producer where Kafka is running.
- `value_serializer=...` converts each Python dictionary into JSON bytes before sending it.
- Kafka stores bytes, so the serializer is required.

### Sample orders

```python
orders = [
    {"customer": "Ali", "product": "Laptop", "price": 1200},
    {"customer": "Sara", "product": "Phone", "price": 800},
    {"customer": "Ali", "product": "Mouse", "price": 25},
    {"customer": "Mona", "product": "Laptop", "price": 1100},
    {"customer": "Sara", "product": "Keyboard", "price": 75},
    {"customer": "Omar", "product": "Phone", "price": 650},
]
```

This list simulates order events from a small e-commerce app. Each dictionary becomes one Kafka message.

### Sending the data

```python
for order in orders:
    producer.send("orders", value=order)
    print("Sent:", order)
    time.sleep(2)
```

- The loop sends each order to the `orders` topic.
- `producer.send(...)` publishes the message to Kafka.
- `print("Sent:", order)` gives visible confirmation in the terminal.
- `time.sleep(2)` slows the stream down so the Spark output is easier to follow during the workshop.

### Finish cleanly

```python
producer.flush()
producer.close()
```

- `flush()` waits until all queued messages are sent.
- `close()` closes the Kafka connection cleanly.

## `spark_kafka_consumer.py`

### Imports and file-level settings

```python
# pyright: reportMissingImports=false

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
```

- The `pyright` comment silences editor warnings in environments where PySpark is not installed for static analysis.
- `os` is used to build the checkpoint path.
- `SparkSession` starts Spark.
- `col` and `from_json` are used to transform Kafka data.
- The type imports define the schema for the JSON order messages.

```python
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "orders"
DEFAULT_APP_NAME = "KafkaSparkOrdersWorkshop"
WORKSHOP_TASK = "incoming"
CHECKPOINT_DIR = ".checkpoints"
```

- These constants keep the important configuration in one place.
- `WORKSHOP_TASK` controls which workshop view runs.
- `CHECKPOINT_DIR` stores Spark streaming state between runs.

### Start Spark

```python
def build_spark_session():
    spark = SparkSession.builder.appName(DEFAULT_APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
```

- `appName(...)` labels the Spark application.
- `getOrCreate()` starts Spark or reuses an existing session.
- `setLogLevel("WARN")` reduces noisy logs so the workshop output stays readable.

### Read Kafka and parse JSON

```python
def build_orders_df(spark: SparkSession):
    schema = StructType(
        [
            StructField("customer", StringType(), True),
            StructField("product", StringType(), True),
            StructField("price", IntegerType(), True),
        ]
    )
```

- `schema` defines the shape of each order message.
- `customer` and `product` are strings.
- `price` is an integer.

```python
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", DEFAULT_BOOTSTRAP_SERVERS)
        .option("subscribe", DEFAULT_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
```

- `readStream.format("kafka")` tells Spark to read Kafka as a stream.
- `kafka.bootstrap.servers` points Spark to the Kafka broker.
- `subscribe` chooses the `orders` topic.
- `startingOffsets="latest"` means Spark starts with new messages, not old ones already in the topic.

```python
    json_df = raw_df.selectExpr("CAST(value AS STRING) AS json_value")
    return json_df.select(from_json(col("json_value"), schema).alias("data")).select(
        "data.*"
    )
```

- Kafka message values arrive as bytes.
- `CAST(value AS STRING)` turns them into text.
- `from_json(...)` parses the JSON using the schema.
- `select("data.*")` expands the parsed fields into normal columns.

### Choose the workshop view

```python
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
```

- `incoming` shows the full parsed row.
- `products` keeps only the product column.
- `expensive` filters to orders above 500.
- `discounted` adds a calculated discount column.
- `count` is the bonus task and groups by customer.
- The final `raise` protects against invalid task names.

### Run the streaming query

```python
def main():
    spark = build_spark_session()
    orders_df = build_orders_df(spark)
    task_df = build_task_df(orders_df, WORKSHOP_TASK)
```

- `main()` ties everything together.
- It starts Spark, reads the Kafka stream, and selects the active workshop view.

```python
    output_mode = "complete" if WORKSHOP_TASK == "count" else "append"
    checkpoint_location = os.path.join(CHECKPOINT_DIR, WORKSHOP_TASK)
```

- `append` is used for row-by-row output.
- `complete` is used for the bonus aggregation so Spark prints the full totals table.
- The checkpoint path is separated by task so each mode keeps its own state.

```python
    query = (
        task_df.writeStream.outputMode(output_mode)
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )
```

- `writeStream` starts the streaming output.
- `format("console")` prints results in the terminal.
- `truncate=False` keeps long values visible.
- `checkpointLocation` lets Spark save progress for streaming recovery.

```python
    print(f"Running workshop task: {WORKSHOP_TASK}")
    print(f"Reading from Kafka topic '{DEFAULT_TOPIC}' at {DEFAULT_BOOTSTRAP_SERVERS}")
    query.awaitTermination()
```

- The `print(...)` lines make the active mode obvious in the terminal.
- `awaitTermination()` keeps the consumer running until you stop it.

### Entry point

```python
if __name__ == "__main__":
    main()
```

- This means the script runs `main()` only when executed directly.
- It keeps the file import-safe if you ever reuse its functions elsewhere.

## How To Explain The Whole Pipeline

In one sentence:

`producer_orders.py` creates JSON order events, Kafka stores them in the `orders` topic, and `spark_kafka_consumer.py` reads those events as a stream and prints different analyses in the console.
