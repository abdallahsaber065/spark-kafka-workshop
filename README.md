# Spark Kafka Workshop

This workspace contains the files needed for the workshop in the PDF:

- `docker-compose.yml` starts ZooKeeper and Kafka locally.
- `producer_orders.py` sends the sample order events to the `orders` topic.
- `spark_kafka_consumer.py` is the starter Spark consumer from the PDF.

## Run order

1. Start Kafka with `docker compose up -d`.
2. Create the `orders` topic.
3. Run the Spark consumer first.
4. Run the producer in another terminal.

Run the Spark consumer with the command from the PDF:

`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_kafka_consumer.py`

## Consumer modes

Follow the PDF tasks by editing `spark_kafka_consumer.py` to show incoming orders, then product names, then filtered orders, then discounted prices, and finally the bonus count per customer.

## Short explanation

Kafka acts as the message broker that stores the order events briefly and lets another process read them later. Spark acts as the streaming consumer and processor that reads the events, transforms them, and prints results in real time.