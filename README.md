# Real-time Twitter Data Processing Pipeline

A comprehensive system for processing Twitter data in real-time using Kafka, Spark Streaming, and PostgreSQL, with performance monitoring and visualization capabilities.

## Project Components

### 1. Data Collection & Publishing (`kafka_producer.py`)
- Reads pre-collected tweets from a dataset
- Publishes tweets to Kafka topic "tweets_topic"
- Acts as the data source for the streaming pipeline

### 2. Database Management (`db_utils.py`)
- Handles all PostgreSQL database operations
- Creates and manages the `tweets` table
- Provides methods for database operations and error handling

### 3. Real-time Processing (`spark_streaming.py`)
- Main streaming application
- Consumes tweets from Kafka
- Processes data in real-time
- Calculates various metrics
- Stores processed data in PostgreSQL

### 4. Performance Monitoring (`performance_monitor.py`)
- Tracks system performance metrics
- Records metrics over time
- Generates performance reports

### 5. Metrics Comparison (`compare_metrics.py`)
- Compares streaming vs batch metrics
- Displays comparison in tabular format

### 6. Visualization (`visualize_metrics.py`)
- Generates visualizations for performance analysis
- Creates various plots and charts

<<<<<<< HEAD
## Prerequisites

- Python 3.8+
- Apache Kafka
- Apache Spark
- PostgreSQL
- Required Python packages (listed in requirements.txt)

## Installation
=======
## Setup
>>>>>>> 06af70ca2b5cf7d76db3b0740d247f420a8dcf60

1. Clone the repository:
```bash
git clone <repository-url>
cd <repository-name>
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Set up PostgreSQL:
```bash
# Create database
sudo -u postgres createdb twitter_data

# Create user and grant permissions
sudo -u postgres psql
CREATE USER your_username WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE twitter_data TO your_username;
```

4. Configure Kafka:
```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
bin/kafka-server-start.sh config/server.properties

# Create topic
bin/kafka-topics.sh --create --topic tweets_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Usage

1. Start the Kafka producer:
```bash
python kafka_producer.py
```

2. Run the Spark streaming application:
```bash
python spark_streaming.py
```

3. Compare metrics:
```bash
python compare_metrics.py
```

4. Generate visualizations:
```bash
python visualize_metrics.py
```

## Project Structure

```
.
├── kafka_producer.py      # Publishes tweets to Kafka
├── spark_streaming.py     # Processes tweets in real-time
├── db_utils.py           # Database operations
├── performance_monitor.py # Tracks system performance
├── compare_metrics.py    # Compares streaming vs batch metrics
├── visualize_metrics.py  # Generates visualizations
├── requirements.txt      # Project dependencies
└── README.md            # Project documentation
```

## Features

- Real-time tweet processing
- Performance monitoring
- Metrics comparison
- Data visualization
- Error handling
- Database management

## Metrics Tracked

<<<<<<< HEAD
1. **Tweet Metrics**
   - Total tweets processed
   - Engagement metrics (likes, retweets, replies)
   - Language distribution
   - User activity

2. **Performance Metrics**
   - Execution time
   - CPU usage
   - Memory usage
   - Disk I/O
   - Network I/O

## Visualizations Generated

1. Performance Comparison
   - Streaming vs Batch execution time
   - CPU usage comparison
   - Memory usage comparison

2. Metrics Trends
   - CPU usage over time
   - Memory usage over time

3. Engagement Metrics
   - Top users by engagement
   - Engagement distribution

4. Language Distribution
   - Tweet language distribution
   - Language comparison between modes

## Error Handling

The system includes comprehensive error handling for:
- Database connections
- Kafka operations
- Spark processing
- File operations
- Metrics calculation

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 
=======
- Processed tweets saved to PostgreSQL
- Real-time analytics displayed in CLI
- Batch vs Streaming performance metrics 
>>>>>>> 06af70ca2b5cf7d76db3b0740d247f420a8dcf60
