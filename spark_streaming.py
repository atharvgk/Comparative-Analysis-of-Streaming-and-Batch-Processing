from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
import json
import re
import os
from datetime import datetime
from db_utils import DatabaseManager

def create_spark_session():
    return SparkSession.builder \
        .appName("ChatGPTAnalysis") \
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
            
            # Process each tweet
            for row in df.collect():
                tweet_data = row.value
                
                # Insert tweet data
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
                
                # Calculate total engagement
                total_engagement = (
                    int(tweet_data.get('reply_count', 0)) +
                    int(tweet_data.get('retweet_count', 0)) +
                    int(tweet_data.get('like_count', 0)) +
                    int(tweet_data.get('quote_count', 0))
                )
                
                # Insert engagement metrics
                db.insert_engagement_metrics({
                    'tweet_id': tweet_data.get('tweet_id', ''),
                    'username': tweet_data.get('username', ''),
                    'total_engagement': total_engagement
                })
                
                # Process hashtags
                hashtags = extract_hashtags(tweet_data.get('text', ''))
                for hashtag in hashtags:
                    db.insert_hashtag_counts({
                        'hashtag': hashtag,
                        'count': 1
                    })
                
                # Insert language distribution
                db.insert_language_distribution({
                    'language': tweet_data.get('language', 'unknown'),
                    'count': 1
                })
            
            # Get and print top hashtags
            top_hashtags = db.get_top_hashtags(10)
            print("\n=== Top 10 Hashtags ===")
            for tag, count in top_hashtags:
                print(f"{tag}: {count}")
            
            # Get and print top engagements
            top_engagements = db.get_top_engagements(10)
            print("\n=== Top 10 Most Engaging Tweets ===")
            for username, total in top_engagements:
                print(f"User: {username}, Engagement: {total}")
            
        except Exception as e:
            print(f"Error processing data: {e}")
        finally:
            db.disconnect()

def main():
    # Create Spark session
    spark = create_spark_session()
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 10)  # 10-second batch interval
    
    # Read from Kafka with latest offset
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "tweets_topic") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Convert the value column from binary to string
    df = df.selectExpr("CAST(value AS STRING)")
    
    # Convert string to JSON
    df = df.select(from_json(df.value, "MAP<STRING,STRING>").alias("data"))
    
    # Process the stream
    query = df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: process_rdd(batch_df.rdd)) \
        .option("checkpointLocation", "checkpoint") \
        .start()
    
    print("Starting streaming application...")
    print("Press Ctrl+C to stop")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming application...")
        query.stop()
        spark.stop()

if __name__ == "__main__":
    main() 