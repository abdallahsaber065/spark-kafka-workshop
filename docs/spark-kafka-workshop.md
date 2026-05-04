# Spark + Kafka Workshop Guide

This project is a small real-time data pipeline:

`producer_orders.py` -> Kafka topic `orders` -> `spark_kafka_consumer.py` -> console output

The workshop shows how an application can publish events, how Kafka can hold those events safely, and how Spark Structured Streaming can read and analyze them while they are still arriving.

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

## Setup With `uv`

Install the Python dependencies:

```bash
uv sync
```

`uv` will create the environment and install the Python packages from `pyproject.toml`.

Start Kafka:

```bash
docker compose up -d
```

Create the topic:

```bash
docker compose exec kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Check the topic:

```bash
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

## How To Run The Workshop

Run the Spark consumer first so it waits for new messages:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_kafka_consumer.py
```

In another terminal, send the sample orders:

```bash
uv run python producer_orders.py
```

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

## How The Consumer Works

The consumer follows the same flow as the PDF:

1. Connect to Kafka with `spark.readStream.format("kafka")`.
2. Read the `orders` topic.
3. Convert each Kafka message from bytes into a JSON string.
4. Parse the JSON into `customer`, `product`, and `price`.
5. Apply the selected workshop task.
6. Print the result to the console.

The JSON schema is simple:

- `customer`: who placed the order,
- `product`: what they bought,
- `price`: the order price.

## How To Explain It To The TA

If you need a short explanation, use this:

Kafka is the message broker that stores events. Spark is the streaming engine that reads those events and analyzes them. The producer sends order messages into Kafka, and the Spark consumer reads them and shows different views of the data in real time.

## Common Questions

- Why do we need Kafka? To hold and deliver events between the app and the stream processor.
- Why not send data directly to Spark? Because Kafka makes the pipeline more flexible and reliable.
- What is streaming data? Data that arrives continuously.
- What is batch data? Data that is collected first and processed later.
- What happens while Spark is running and new orders arrive? Spark keeps reading and printing the new events.

## Common Troubleshooting

- If Spark shows no output, start the consumer first, then run the producer.
- If Kafka cannot connect, confirm the container is running and port `9092` is available.
- If Docker says a container name is already in use, it usually means an older Kafka container is still around. Remove it once with `docker rm -f kafka` or run `docker compose down` in the old project folder.
- If the Kafka package is missing, make sure the Spark package version matches your Spark installation.
- If the topic does not exist, create the `orders` topic again.
