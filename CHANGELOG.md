# Changelog

## 2026-05-05

- Cleaned the Spark Kafka workshop consumer into a runnable CLI-based script with modes for incoming orders, product names, expensive orders, discounted prices, and customer counts.
- Added `docs/spark-kafka-workshop.md` to explain Kafka, Spark, the streaming pipeline, setup steps, and workshop usage for TA discussion.
- Tuned the Docker Compose Kafka and ZooKeeper JVM heaps down for low-memory hosts like a 1 GB EC2 instance without swap.
- Pinned the workshop to Spark/PySpark 3.5.1, added Java 17 and Python 3.11 setup guidance, and updated the run commands to use `uv`.
- Moved setup/run instructions into `README.md` and refocused `docs/spark-kafka-workshop.md` on internal code and command explanations.
- Added the workshop discussion questions and short answers to `docs/spark-kafka-workshop.md`.
- Added `docs/script-walkthrough.md` with a dedicated explanation of `producer_orders.py` and `spark_kafka_consumer.py`.
