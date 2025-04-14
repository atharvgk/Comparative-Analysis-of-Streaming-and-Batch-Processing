from kafka import KafkaProducer
import pandas as pd
import json
import time

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'tweets_topic'

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def read_tweets_from_csv(file_path):
    return pd.read_csv(file_path)

def publish_tweets(producer, tweets_df):
    for _, tweet in tweets_df.iterrows():
        try:
            # Convert tweet to dictionary and send to Kafka
            tweet_data = {
                'text': str(tweet['Text']),
                'username': str(tweet['Username']),
                'tweet_id': str(tweet['Tweet Id']),
                'language': str(tweet['Language']),
                'source': str(tweet['Source']),
                'reply_count': int(tweet['ReplyCount']) if pd.notna(tweet['ReplyCount']) else 0,
                'retweet_count': int(tweet['RetweetCount']) if pd.notna(tweet['RetweetCount']) else 0,
                'like_count': int(tweet['LikeCount']) if pd.notna(tweet['LikeCount']) else 0,
                'quote_count': int(tweet['QuoteCount']) if pd.notna(tweet['QuoteCount']) else 0,
                'hashtags': str(tweet['hashtag']) if pd.notna(tweet['hashtag']) else '',
                'hashtag_counts': int(tweet['hastag_counts']) if pd.notna(tweet['hastag_counts']) else 0
            }
            producer.send(TOPIC_NAME, value=tweet_data)
            print(f"Published tweet: {tweet_data['text'][:50]}...")
            time.sleep(0.1)  # Small delay to prevent overwhelming Kafka
        except Exception as e:
            print(f"Error processing tweet: {e}")
            continue

def main():
    producer = create_kafka_producer()
    tweets_df = read_tweets_from_csv('chatgpt1.csv')
    
    try:
        publish_tweets(producer, tweets_df)
    except Exception as e:
        print(f"Error publishing tweets: {e}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main() 