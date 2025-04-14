import psycopg2

def clear_database():
    try:
        # Connect to the database
        conn = psycopg2.connect(
            dbname="twitter_data",
            user="postgres",
            password="postgres",
            host="localhost"
        )
        
        # Create a cursor
        cur = conn.cursor()
        
        # Clear all tables
        tables = ['tweets', 'hashtag_counts', 'engagement_metrics', 'language_distribution']
        for table in tables:
            cur.execute(f"TRUNCATE TABLE {table} CASCADE;")
            print(f"Cleared table: {table}")
        
        # Commit the changes
        conn.commit()
        
        # Close the cursor and connection
        cur.close()
        conn.close()
        
        print("\nDatabase cleared successfully!")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    clear_database() 