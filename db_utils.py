import psycopg2
from psycopg2 import sql
import pandas as pd
from datetime import datetime

class DatabaseManager:
    def __init__(self, dbname="twitter_data", user="postgres", password="postgres", host="localhost", port="5432"):
        self.connection_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            self.cursor = self.conn.cursor()
            print("Connected to PostgreSQL database")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("Disconnected from PostgreSQL database")

    def insert_tweet(self, tweet_data):
        try:
            query = """
                INSERT INTO tweets (
                    tweet_id, text, username, language, source,
                    reply_count, retweet_count, like_count, quote_count,
                    hashtags, processing_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.cursor.execute(query, (
                tweet_data.get('tweet_id'),
                tweet_data.get('text'),
                tweet_data.get('username'),
                tweet_data.get('language'),
                tweet_data.get('source'),
                tweet_data.get('reply_count'),
                tweet_data.get('retweet_count'),
                tweet_data.get('like_count'),
                tweet_data.get('quote_count'),
                tweet_data.get('hashtag'),
                datetime.now()
            ))
            self.conn.commit()
        except Exception as e:
            print(f"Error inserting tweet: {e}")
            self.conn.rollback()

    def insert_hashtag_counts(self, data):
        try:
            # Check if hashtag exists
            self.cursor.execute("""
                SELECT id, count FROM hashtag_counts 
                WHERE hashtag = %s
            """, (data['hashtag'],))
            
            result = self.cursor.fetchone()
            
            if result:
                # Update existing count
                self.cursor.execute("""
                    UPDATE hashtag_counts 
                    SET count = count + 1, 
                        timestamp = %s 
                    WHERE id = %s
                """, (datetime.now(), result[0]))
            else:
                # Insert new hashtag
                self.cursor.execute("""
                    INSERT INTO hashtag_counts 
                    (hashtag, count, timestamp) 
                    VALUES (%s, 1, %s)
                """, (data['hashtag'], datetime.now()))
            
            self.conn.commit()
        except Exception as e:
            print(f"Error inserting hashtag count: {e}")
            self.conn.rollback()

    def insert_engagement_metrics(self, engagement_data):
        try:
            query = """
                INSERT INTO engagement_metrics (
                    tweet_id, username, total_engagement, timestamp
                ) VALUES (%s, %s, %s, %s)
            """
            self.cursor.execute(query, (
                engagement_data.get('tweet_id'),
                engagement_data.get('username'),
                engagement_data.get('total_engagement'),
                datetime.now()
            ))
            self.conn.commit()
        except Exception as e:
            print(f"Error inserting engagement metrics: {e}")
            self.conn.rollback()

    def insert_language_distribution(self, language_data):
        try:
            query = """
                INSERT INTO language_distribution (
                    language, count, timestamp
                ) VALUES (%s, %s, %s)
            """
            self.cursor.execute(query, (
                language_data.get('language'),
                language_data.get('count'),
                datetime.now()
            ))
            self.conn.commit()
        except Exception as e:
            print(f"Error inserting language distribution: {e}")
            self.conn.rollback()

    def get_tweets(self, limit=100):
        try:
            query = "SELECT * FROM tweets ORDER BY processing_time DESC LIMIT %s"
            self.cursor.execute(query, (limit,))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error fetching tweets: {e}")
            return []

    def get_top_hashtags(self, limit=10):
        try:
            self.cursor.execute("""
                SELECT hashtag, count 
                FROM hashtag_counts 
                ORDER BY count DESC 
                LIMIT %s
            """, (limit,))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error getting top hashtags: {e}")
            return []

    def get_top_engagements(self, limit=10):
        try:
            query = """
                SELECT username, SUM(total_engagement) as total
                FROM engagement_metrics
                GROUP BY username
                ORDER BY total DESC
                LIMIT %s
            """
            self.cursor.execute(query, (limit,))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error fetching top engagements: {e}")
            return [] 