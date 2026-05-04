# Changelog

## 2026-05-05

- Cleaned the Spark Kafka workshop consumer into a runnable CLI-based script with modes for incoming orders, product names, expensive orders, discounted prices, and customer counts.
- Added `docs/spark-kafka-workshop.md` to explain Kafka, Spark, the streaming pipeline, setup steps, and workshop usage for TA discussion.
- Tuned the Docker Compose Kafka and ZooKeeper JVM heaps down for low-memory hosts like a 1 GB EC2 instance without swap.
- Pinned the workshop to Spark/PySpark 3.5.1, added Java 17 and Python 3.11 setup guidance, and updated the run commands to use `uv`.
