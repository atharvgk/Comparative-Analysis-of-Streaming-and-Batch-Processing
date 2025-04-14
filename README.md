# Twitter Data Processing Pipeline

This project implements a real-time data processing pipeline for Twitter data using Kafka and Spark Streaming.

## Components

1. **Kafka Setup**
   - Producer: Reads tweets from CSV and publishes to Kafka
   - Consumer: Consumes tweets from Kafka and saves to CSV

2. **Spark Streaming**
   - Real-time processing of tweets
   - Hashtag analysis and counting
   - Results saved to CSV files

## Prerequisites

1. Apache Kafka (installed and running)
2. Apache Spark (installed and running)
3. Python 3.x
4. Required Python packages (install using `pip install -r requirements.txt`)

## Setup Instructions

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Make sure Kafka and Zookeeper are running:
   ```bash
   # Start Zookeeper
   /usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties

   # Start Kafka
   /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties
   ```

3. Create Kafka topic (if not already created):
   ```bash
   /usr/local/kafka/bin/kafka-topics.sh --create --topic tweets_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

## Running the Application

1. Start the Kafka Producer:
   ```bash
   python kafka_producer.py
   ```

2. Start the Spark Streaming application:
   ```bash
   python spark_streaming.py
   ```

3. (Optional) Start the Kafka Consumer:
   ```bash
   python kafka_consumer.py
   ```

## Output

- The Spark Streaming application will:
  - Print top 10 hashtags every 10 seconds
  - Save hashtag counts to the `hashtag_counts` directory
- The Kafka Consumer will save processed tweets to `processed_tweets.csv`

## Notes

- The Spark Streaming application processes data in 10-second batches
- Hashtag analysis includes case-insensitive counting
- Results are saved in CSV format for further analysis 