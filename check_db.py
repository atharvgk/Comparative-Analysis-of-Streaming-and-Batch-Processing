import psycopg2
from psycopg2 import sql

def check_database():
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
        
        # Get all tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public';
        """)
        
        tables = cur.fetchall()
        
        # For each table, get and display data
        for table in tables:
            table_name = table[0]
            print(f"\n{'='*50}")
            print(f"Data from table: {table_name}")
            print(f"{'='*50}")
            
            # Get column names
            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}';
            """)
            columns = [col[0] for col in cur.fetchall()]
            
            # Get data
            cur.execute(f"SELECT * FROM {table_name} LIMIT 5;")
            rows = cur.fetchall()
            
            if not rows:
                print("No data found in this table")
                continue
                
            # Print column headers
            print(" | ".join(columns))
            print("-" * 50)
            
            # Print data rows
            for row in rows:
                print(" | ".join(str(value) for value in row))
            
            # Print total count
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cur.fetchone()[0]
            print(f"\nTotal records in {table_name}: {count}")
        
        # Close the cursor and connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_database() 