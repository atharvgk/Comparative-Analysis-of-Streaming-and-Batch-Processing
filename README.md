# Twitter Data Processing Pipeline

A real-time data processing pipeline for Twitter data using Kafka and Spark Streaming.

## Prerequisites

- Apache Kafka
- Apache Spark
- PostgreSQL
- Python 3.x

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Start Kafka services:
   ```bash
   # Start Zookeeper
   /usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties

   # Start Kafka
   /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties
   ```

3. Create Kafka topic:
   ```bash
   /usr/local/kafka/bin/kafka-topics.sh --create --topic tweets_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

4. Setup database:
   ```bash
   python setup_database.py
   ```

## Running the Application

1. Start Kafka Producer:
   ```bash
   python kafka_producer.py
   ```

2. Start Spark Streaming:
   ```bash
   python spark_streaming.py
   ```

3. (Optional) Start Kafka Consumer:
   ```bash
   python kafka_consumer.py
   ```

4. Run Batch Processing:
   ```bash
   python batch_processing.py
   ```

## Features

- Real-time tweet processing
- Hashtag analysis
- Engagement metrics
- Language distribution
- Performance comparison between batch and streaming modes

## Output

- Processed tweets saved to PostgreSQL
- Real-time analytics displayed in console
- Batch vs Streaming performance metrics 