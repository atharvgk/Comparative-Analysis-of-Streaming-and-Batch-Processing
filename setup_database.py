import psycopg2
from psycopg2 import sql

def create_database():
    # Connect to default database to create our database
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="localhost"
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # Create database if it doesn't exist
    cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'twitter_data'")
    exists = cur.fetchone()
    if not exists:
        cur.execute("CREATE DATABASE twitter_data")
        print("Database 'twitter_data' created successfully")
    else:
        print("Database 'twitter_data' already exists")
    
    cur.close()
    conn.close()

def create_tables():
    # Connect to our database
    conn = psycopg2.connect(
        dbname="twitter_data",
        user="postgres",
        password="postgres",
        host="localhost"
    )
    cur = conn.cursor()
    
    # Create tweets table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tweets (
            id SERIAL PRIMARY KEY,
            tweet_id VARCHAR(255),
            text TEXT,
            username VARCHAR(255),
            language VARCHAR(50),
            source VARCHAR(255),
            reply_count INTEGER,
            retweet_count INTEGER,
            like_count INTEGER,
            quote_count INTEGER,
            hashtags TEXT,
            hashtag_count INTEGER,
            processing_time TIMESTAMP
        )
    """)
    
    # Create hashtag_counts table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hashtag_counts (
            id SERIAL PRIMARY KEY,
            hashtag VARCHAR(255),
            count INTEGER,
            timestamp TIMESTAMP
        )
    """)
    
    # Create engagement_metrics table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS engagement_metrics (
            id SERIAL PRIMARY KEY,
            tweet_id VARCHAR(255),
            username VARCHAR(255),
            total_engagement INTEGER,
            language VARCHAR(50),
            source VARCHAR(255),
            timestamp TIMESTAMP
        )
    """)
    
    # Create language_distribution table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS language_distribution (
            id SERIAL PRIMARY KEY,
            language VARCHAR(50),
            count INTEGER,
            timestamp TIMESTAMP
        )
    """)
    
    conn.commit()
    print("Tables created successfully")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    create_database()
    create_tables() 