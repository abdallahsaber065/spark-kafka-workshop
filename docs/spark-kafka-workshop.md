# Spark + Kafka Workshop Guide

This project is a small real-time data pipeline:

`producer_orders.py` -> Kafka topic `orders` -> `spark_kafka_consumer.py` -> console output

The workshop shows how an application can publish events, how Kafka can hold those events safely, and how Spark Structured Streaming can read and analyze them while they are still arriving.

## Recommended Versions

For this workshop, the safest version combination is:

- Python 3.10 or 3.11
- Java 17
- `pyspark==3.5.1`
- `spark-sql-kafka-0-10_2.12:3.5.1`

Spark 3.5.1 supports Java 8/11/17 and Python 3.8+, but Python 3.12 and 3.13 are not a good fit for this workshop. That is why this project now pins PySpark to 3.5.1 and avoids newer Python versions.

## What Is Kafka?

Kafka is an event streaming platform. In simple terms, it is a place where programs can send messages and other programs can read them later.

For this workshop, Kafka stores order events in the `orders` topic.

Think of Kafka like a durable mailbox:

- the producer drops messages in the mailbox,
- Kafka keeps them available,
- Spark comes later and reads them.

Why teams use Kafka in real work:

- it separates the system that creates events from the system that analyzes them,
- it can buffer high volumes of data,
- it helps if a consumer is slow, paused, or restarted,
- it is common in e-commerce, payments, logs, notifications, and clickstream tracking.

## What Is Spark?

Spark is a data processing engine. Spark Structured Streaming is the part of Spark that keeps reading new data as it arrives.

For this workshop, Spark reads the Kafka topic, turns each JSON message into columns, and prints the result to the console.

Think of Spark like the analysis worker:

- it reads the incoming events,
- it transforms them,
- it can filter, reshape, and count them,
- it can work continuously instead of waiting for a full batch file.

Why teams use Spark in real work:

- it processes large amounts of data,
- it supports both batch and streaming work,
- it is useful for reporting, dashboards, alerts, fraud checks, and data pipelines.

## Why Use Kafka And Spark Together?

Kafka handles delivery and buffering. Spark handles reading and analysis.

That split is useful because a real application often needs to write events immediately, while analysis systems may run slower, restart, or apply different transformations later.

In this workshop:

1. `producer_orders.py` creates sample order events.
2. Kafka stores them in the `orders` topic.
3. Spark reads those events as a stream.
4. Spark prints different views of the same data for the class tasks.

## Project Files

- `docker-compose.yml` starts ZooKeeper and Kafka locally.
- `producer_orders.py` sends sample order events to Kafka.
- `spark_kafka_consumer.py` reads the Kafka stream and shows the workshop tasks.
- `pyproject.toml` defines the Python dependencies for `uv`.

## Workshop Tasks

`spark_kafka_consumer.py` is set to one task at a time through the `WORKSHOP_TASK` variable near the top of the file.

- `incoming` shows all parsed orders.
- `products` shows only the product column.
- `expensive` shows only orders where `price > 500`.
- `discounted` adds a `discounted_price` column with a 10% discount.
- `count` is the bonus task and counts orders per customer.

Example:

```python
WORKSHOP_TASK = "count"
```

The bonus task is already included in the code. It uses `groupBy("customer").count()` and Spark `complete` output mode so the full totals table is printed each time data changes.

## Command Notes

These are the main commands the project uses and what each one does:

- `uv sync` creates the local Python environment and installs the packages from `pyproject.toml`.
- `uv run python producer_orders.py` runs the producer inside that environment.
- `uv run spark-submit ... spark_kafka_consumer.py` launches Spark then loads the Kafka connector package for Structured Streaming.
- `docker compose up -d` starts ZooKeeper and Kafka in the background.
- `docker compose exec kafka kafka-topics ...` runs Kafka CLI commands inside the Kafka container so we can create and inspect the `orders` topic.

## Code Walkthrough

### `producer_orders.py`

The producer is a short script that creates a `KafkaProducer`, converts Python dictionaries into JSON, and sends them to the `orders` topic. The `time.sleep(2)` call is only there to make the messages appear one by one during the workshop, so the stream is easier to follow on screen.

### `spark_kafka_consumer.py`

The consumer does the real workshop work:

1. It starts a `SparkSession` and lowers Spark logging noise.
2. It reads the Kafka topic with `spark.readStream.format("kafka")`.
3. It casts the Kafka `value` column from bytes to text.
4. It parses the JSON into the `customer`, `product`, and `price` columns.
5. It chooses one task view based on `WORKSHOP_TASK`.
6. It writes the result to the console so the class can see the stream live.

The `WORKSHOP_TASK` variable is the part to change when you want a different view:

- `incoming` shows the full parsed order row.
- `products` keeps only the product name.
- `expensive` filters to rows where `price > 500`.
- `discounted` adds a `discounted_price` column using `price * 0.9`.
- `count` is the bonus task and groups by customer.

The schema in the consumer is intentionally small because the workshop is about the streaming flow, not about a complicated data model.

## Why The Stream Is Written This Way

The consumer uses `append` output mode for the row-by-row tasks because each new Kafka record is a new result row. The bonus aggregation uses `complete` output mode because Spark needs to print the full group-by table every time the counts change.

## How To Explain It To The TA

If you need a short explanation, use this:

Kafka is the message broker that stores events. Spark is the streaming engine that reads those events and analyzes them. The producer sends order messages into Kafka, and the Spark consumer reads them and shows different views of the data in real time.

## Discussion Questions

1. Why do we need Kafka between the application and Spark?

   Kafka sits between the producer and Spark so the app can send events immediately while Spark reads them later. It buffers the data, decouples the two sides, and makes the pipeline more reliable if Spark is slow or restarts.

2. What is the difference between batch data and streaming data?

   Batch data is collected first and processed later in a group. Streaming data arrives continuously and is processed while it is still coming in.

3. What happens if the producer sends more orders while Spark is still running?

   Spark keeps listening to the Kafka topic and reads the new orders as they arrive. In the console, you see new rows appear in later micro-batches without stopping the consumer.

4. Which part of the pipeline is the producer? Which part is the consumer?

   `producer_orders.py` is the producer because it sends the order events to Kafka. `spark_kafka_consumer.py` is the consumer because Spark reads the Kafka topic and processes the events.

## Code & Commands

### Kafka startup and topic creation

```bash
docker compose up -d
docker compose exec kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092 # Check that the topic exists
```

The topic creation command ends with:

```text
Created topic orders.
```

### Producer terminal

```bash
uv run python producer_orders.py
```

output:

```text
Sent: {'customer': 'Omar', 'product': 'Phone', 'price': 650}
Sent: {'customer': 'Sara', 'product': 'Keyboard', 'price': 75}
Sent: {'customer': 'Mona', 'product': 'Laptop', 'price': 1100}
Sent: {'customer': 'Ali', 'product': 'Mouse', 'price': 25}
Sent: {'customer': 'Sara', 'product': 'Phone', 'price': 800}
Sent: {'customer': 'Ali', 'product': 'Laptop', 'price': 1200}
```

### Spark consumer terminal

```bash
uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_kafka_consumer.py
```

output:

```text
Reading from Kafka topic 'orders' at localhost:9092
Running workshop task: incoming
```

```text
Batch: 0
+--------+-------+-----+
|customer|product|price|
+--------+-------+-----+
+--------+-------+-----+
-------------------------------------------

Batch: 1
+--------+-------+-----+
|Ali     |Laptop |1200 |
+--------+-------+-----+
|customer|product|price|
+--------+-------+-----+
-------------------------------------------

Batch: 2
+--------+-------+-----+
|Ali     |Mouse  |25   |
|Sara    |Phone  |800  |
+--------+-------+-----+
|customer|product|price|
+--------+-------+-----+
-------------------------------------------

Batch: 3
+--------+-------+-----+
|Mona    |Laptop |1100 |
+--------+-------+-----+
|customer|product|price|
+--------+-------+-----+
-------------------------------------------

Batch: 4
+--------+--------+-----+
|Sara    |Keyboard|75   |
+--------+--------+-----+
|customer|product |price|
+--------+--------+-----+
-------------------------------------------

Batch: 5
+--------+-------+-----+
|Omar    |Phone  |650  |
+--------+-------+-----+
|customer|product|price|
+--------+-------+-----+
-------------------------------------------
```

### What this snapshot shows

- `docker compose up -d` starts Kafka and ZooKeeper.
- Kafka accepts the `orders` topic.
- The producer sends order events one by one.
- Spark reads the topic and prints each batch as it arrives.
- The changing batch numbers show that this is a streaming pipeline, not a batch job.