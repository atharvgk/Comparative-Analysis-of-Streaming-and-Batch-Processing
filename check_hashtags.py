import psycopg2

def check_hashtags():
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
        
        # Get first 5 entries from hashtag_counts
        cur.execute("""
            SELECT id, hashtag, count, timestamp 
            FROM hashtag_counts 
            ORDER BY id 
            LIMIT 5
        """)
        
        # Print the results
        print("\nFirst 5 entries in hashtag_counts table:")
        print("-" * 50)
        print("ID | Hashtag | Count | Timestamp")
        print("-" * 50)
        
        for row in cur.fetchall():
            print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]}")
        
        # Close the cursor and connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_hashtags() 