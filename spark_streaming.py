from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
import json
import re
import os
from datetime import datetime
from db_utils import DatabaseManager
from performance_monitor import PerformanceMonitor
import time

def create_spark_session():
    return SparkSession.builder \
        .appName("TwitterStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

def extract_hashtags(text):
    if text is None:
        return []
    return re.findall(r'#\w+', text.lower())

def process_rdd(rdd):
    if not rdd.isEmpty():
        # Initialize database connection
        db = DatabaseManager()
        db.connect()
        
        try:
            # Convert RDD to DataFrame
            df = rdd.toDF(["value"])
            
            # Define schema for the JSON data
            schema = StructType([
                StructField("tweet_id", StringType(), True),
                StructField("text", StringType(), True),
                StructField("username", StringType(), True),
                StructField("language", StringType(), True),
                StructField("source", StringType(), True),
                StructField("reply_count", IntegerType(), True),
                StructField("retweet_count", IntegerType(), True),
                StructField("like_count", IntegerType(), True),
                StructField("quote_count", IntegerType(), True),
                StructField("hashtag", StringType(), True)
            ])
            
            # Parse JSON with schema
            df = df.select(from_json(col("value"), schema).alias("data"))
            
            # Initialize metrics dictionary with previous values
            metrics = {
                'total_tweets': 0,
                'total_engagement': 0,
                'engagement': {},
                'languages': {}
            }
            
            # Read previous metrics from file
            try:
                with open('streaming_metrics.txt', 'r') as f:
                    lines = f.readlines()
                    if lines:
                        # Get the last batch of metrics
                        last_batch = []
                        current_batch = []
                        for line in lines:
                            if line.startswith('=== Streaming Metrics at'):
                                if current_batch:
                                    last_batch = current_batch
                                current_batch = []
                            current_batch.append(line)
                        
                        # Parse previous metrics
                        for line in last_batch:
                            line = line.strip()
                            if not line or line.startswith('===') or line.startswith('='):
                                continue
                                
                            if ':' in line:
                                try:
                                    key, value = line.split(': ', 1)
                                    if key == 'Total Tweets Processed':
                                        metrics['total_tweets'] = int(value)
                                    elif key == 'Total Engagement':
                                        metrics['total_engagement'] = int(value)
                                    elif key.startswith('User:'):
                                        username = key.split('User: ')[1].split(',')[0]
                                        engagement = int(value.split('Engagement: ')[1])
                                        metrics['engagement'][username] = engagement
                                    elif key.startswith('Language:'):
                                        lang = key.split('Language: ')[1].split(',')[0]
                                        count = int(value.split('Count: ')[1])
                                        metrics['languages'][lang] = count
                                except (ValueError, IndexError) as e:
                                    print(f"Warning: Error parsing line '{line}': {e}")
                                    continue
            except FileNotFoundError:
                pass  # First run, no previous metrics
            
            # Process each tweet
            for row in df.collect():
                tweet_data = row.data.asDict()
                
                # Insert tweet data into SQL database
                db.insert_tweet({
                    'tweet_id': tweet_data.get('tweet_id', ''),
                    'text': tweet_data.get('text', ''),
                    'username': tweet_data.get('username', ''),
                    'language': tweet_data.get('language', ''),
                    'source': tweet_data.get('source', ''),
                    'reply_count': int(tweet_data.get('reply_count', 0)),
                    'retweet_count': int(tweet_data.get('retweet_count', 0)),
                    'like_count': int(tweet_data.get('like_count', 0)),
                    'quote_count': int(tweet_data.get('quote_count', 0)),
                    'hashtag': tweet_data.get('hashtag', '')
                })
                
                # Update streaming metrics
                metrics['total_tweets'] += 1
                
                # Calculate engagement
                engagement = (
                    int(tweet_data.get('reply_count', 0)) +
                    int(tweet_data.get('retweet_count', 0)) +
                    int(tweet_data.get('like_count', 0)) +
                    int(tweet_data.get('quote_count', 0))
                )
                metrics['total_engagement'] += engagement
                
                # Update user engagement
                username = tweet_data.get('username', '')
                if username in metrics['engagement']:
                    metrics['engagement'][username] += engagement
                else:
                    metrics['engagement'][username] = engagement
                
                # Update language distribution
                language = tweet_data.get('language', 'unknown')
                if language in metrics['languages']:
                    metrics['languages'][language] += 1
                else:
                    metrics['languages'][language] = 1
            
            # Write metrics to text file
            with open('streaming_metrics.txt', 'a') as f:
                f.write(f"\n=== Streaming Metrics at {datetime.now()} ===\n")
                f.write(f"Total Tweets Processed: {metrics['total_tweets']}\n")
                f.write(f"Total Engagement: {metrics['total_engagement']}\n")
                
                # Write top engaging users
                f.write("\nTop Engaging Users:\n")
                for username, engagement in sorted(metrics['engagement'].items(), 
                                                key=lambda x: x[1], reverse=True)[:10]:
                    f.write(f"User: {username}, Engagement: {engagement}\n")
                
                # Write language distribution
                f.write("\nLanguage Distribution:\n")
                for language, count in sorted(metrics['languages'].items(), 
                                           key=lambda x: x[1], reverse=True)[:10]:
                    f.write(f"Language: {language}, Count: {count}\n")
                
                f.write("\n" + "="*50 + "\n")
            
            # Print current metrics
            print("\n=== Current Streaming Metrics ===")
            print(f"Total Tweets Processed: {metrics['total_tweets']}")
            print(f"Total Engagement: {metrics['total_engagement']}")
            
            # Print top engaging users
            print("\nTop Engaging Users:")
            for username, engagement in sorted(metrics['engagement'].items(),
                                            key=lambda x: x[1], reverse=True)[:10]:
                print(f"User: {username}, Engagement: {engagement}")
            
            # Print language distribution
            print("\nLanguage Distribution:")
            for language, count in sorted(metrics['languages'].items(),
                                       key=lambda x: x[1], reverse=True)[:10]:
                print(f"Language: {language}, Count: {count}")
            
        except Exception as e:
            print(f"Error processing data: {e}")
        finally:
            db.disconnect()

def main():
    # Initialize performance monitor
    perf_monitor = PerformanceMonitor()
    perf_monitor.start_monitoring()
    
    try:
        # Initialize Spark session
        spark = create_spark_session()
        
        # Read from Kafka
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "tweets_topic") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Convert the value column from binary to string
        df = df.selectExpr("CAST(value AS STRING)")
        
        # Process the streaming data
        query = df.writeStream \
            .foreachBatch(lambda batch_df, batch_id: process_rdd(batch_df.rdd)) \
            .start()
        
        # Record metrics periodically
        while query.isActive:
            perf_monitor.record_metrics()
            time.sleep(1)  # Record metrics every second
        
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nStopping streaming application...")
    finally:
        perf_monitor.stop_monitoring()
        perf_monitor.save_metrics('streaming_metrics.json')
        
        # Print performance summary
        summary = perf_monitor.get_summary()
        print("\n=== Streaming Performance Summary ===")
        for key, value in summary.items():
            print(f"{key}: {value}")
        
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main() 