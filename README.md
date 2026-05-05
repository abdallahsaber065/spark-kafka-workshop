# Spark Kafka Workshop

This workspace contains the files needed for the workshop in the PDF:

- `docker-compose.yml` starts ZooKeeper and Kafka locally.
- `producer_orders.py` sends the sample order events to the `orders` topic.
- `spark_kafka_consumer.py` is the Spark consumer for the workshop tasks.
- `docs/spark-kafka-workshop.md` explains Kafka, Spark, the pipeline, and how to run the workshop.
- `docs/script-walkthrough.md` breaks down the producer and consumer scripts section by section.

## Setup

1. Make sure `uv` is installed.
2. Install a compatible Python version if needed:

   ```bash
   uv python install 3.11
   ```

3. Install the Python dependencies:

   ```bash
   uv sync
   ```

4. Install Java 17 and set `JAVA_HOME` to the JDK directory.
5. Start Kafka with Docker:

   ```bash
   docker compose up -d
   ```

6. Create the `orders` topic:

   ```bash
   docker compose exec kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

## Run

Run the Spark consumer first:

```bash
uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_kafka_consumer.py
```

Then send the sample orders from another terminal:

```bash
uv run python producer_orders.py
```

## Workshop Modes

Edit `WORKSHOP_TASK` in `spark_kafka_consumer.py` to switch between the workshop steps:

- `incoming`
- `products`
- `expensive`
- `discounted`
- `count` for the bonus task

For a full beginner-friendly explanation, open `docs/spark-kafka-workshop.md`.
