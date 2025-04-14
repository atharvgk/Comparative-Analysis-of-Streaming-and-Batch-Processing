from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'tweets_topic'
GROUP_ID = 'tweet_group'
OUTPUT_FILE = 'processed_tweets.csv'

def create_kafka_consumer():
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def save_to_csv(df, filename):
    try:
        # Check if file exists
        file_exists = os.path.isfile(filename)
        
        # Save to CSV
        df.to_csv(filename, mode='a', header=not file_exists, index=False)
        print(f"Successfully saved data to {filename}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def process_tweet(tweet_data):
    try:
        # Here you can add your tweet processing logic
        print(f"Processing tweet: {tweet_data.get('text', '')[:50]}...")
        
        # Convert to DataFrame
        df = pd.DataFrame([{
            'Text': tweet_data.get('text', ''),
            'Username': tweet_data.get('username', ''),
            'Tweet Id': tweet_data.get('tweet_id', ''),
            'Language': tweet_data.get('language', ''),
            'Source': tweet_data.get('source', ''),
            'ReplyCount': tweet_data.get('reply_count', 0),
            'RetweetCount': tweet_data.get('retweet_count', 0),
            'LikeCount': tweet_data.get('like_count', 0),
            'QuoteCount': tweet_data.get('quote_count', 0),
            'hashtag': tweet_data.get('hashtags', ''),
            'hastag_counts': tweet_data.get('hashtag_counts', 0),
            'Processing_Time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }])
        
        # Save to CSV
        save_to_csv(df, OUTPUT_FILE)
        
    except Exception as e:
        print(f"Error processing tweet: {e}")

def main():
    consumer = create_kafka_consumer()
    
    try:
        print(f"Starting consumer. Processed tweets will be saved to {OUTPUT_FILE}")
        for message in consumer:
            tweet_data = message.value
            process_tweet(tweet_data)
    except KeyboardInterrupt:
        print("\nConsumer stopped by user")
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        consumer.close()
        print(f"Consumer closed. Final data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main() 