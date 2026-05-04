# Spark Kafka Workshop

This workspace contains the files needed for the workshop in the PDF:

- `docker-compose.yml` starts ZooKeeper and Kafka locally.
- `producer_orders.py` sends the sample order events to the `orders` topic.
- `spark_kafka_consumer.py` is the Spark consumer for the workshop tasks.
- `docs/spark-kafka-workshop.md` explains Kafka, Spark, the pipeline, and how to run the workshop.

## Quick Start

1. Install Python dependencies with `uv sync`.
2. Make sure Java 17 is installed and `JAVA_HOME` points to the JDK directory.
3. Start Kafka with `docker compose up -d`.
4. Create the `orders` topic.
5. Run the Spark consumer with `uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_kafka_consumer.py`.
6. Run the producer in another terminal with `uv run python producer_orders.py`.

The Spark consumer is run with Spark itself through `uv`:

`uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_kafka_consumer.py`

## Workshop Modes

Edit `WORKSHOP_TASK` in `spark_kafka_consumer.py` to switch between the workshop steps:

- `incoming`
- `products`
- `expensive`
- `discounted`
- `count` for the bonus task

For a full beginner-friendly explanation, open `docs/spark-kafka-workshop.md`.
