from pyspark.sql import SparkSession
import time
from datetime import datetime
import psycopg2
from tabulate import tabulate

def create_spark_session():
    return SparkSession.builder \
        .appName("BatchProcessing") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

def get_batch_results(spark):
    # Read from PostgreSQL
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/twitter_data") \
        .option("dbtable", "tweets") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    # Register as temporary view
    df.createOrReplaceTempView("tweets")
    
    # Execute queries and measure time
    queries = {
        "Top Hashtags": """
            SELECT hashtags, COUNT(*) as count 
            FROM tweets 
            WHERE hashtags != '' 
            GROUP BY hashtags 
            ORDER BY count DESC 
            LIMIT 10
        """,
        "Top Engaging Users": """
            SELECT username, 
                   SUM(reply_count + retweet_count + like_count + quote_count) as total_engagement
            FROM tweets
            GROUP BY username
            ORDER BY total_engagement DESC
            LIMIT 10
        """,
        "Language Distribution": """
            SELECT language, COUNT(*) as count
            FROM tweets
            GROUP BY language
            ORDER BY count DESC
            LIMIT 10
        """
    }
    
    results = {}
    execution_times = {}
    
    for name, query in queries.items():
        start_time = time.time()
        result = spark.sql(query)
        execution_time = time.time() - start_time
        execution_times[name] = execution_time
        results[name] = result.collect()
    
    return results, execution_times

def get_streaming_results():
    try:
        conn = psycopg2.connect(
            dbname="twitter_data",
            user="postgres",
            password="postgres",
            host="localhost"
        )
        cur = conn.cursor()
        
        queries = {
            "Top Hashtags": """
                SELECT hashtag, count 
                FROM hashtag_counts 
                ORDER BY count DESC 
                LIMIT 10
            """,
            "Top Engaging Users": """
                SELECT username, total_engagement
                FROM engagement_metrics
                ORDER BY total_engagement DESC
                LIMIT 10
            """,
            "Language Distribution": """
                SELECT language, count
                FROM language_distribution
                ORDER BY count DESC
                LIMIT 10
            """
        }
        
        results = {}
        execution_times = {}
        
        for name, query in queries.items():
            start_time = time.time()
            cur.execute(query)
            results[name] = cur.fetchall()
            execution_time = time.time() - start_time
            execution_times[name] = execution_time
        
        cur.close()
        conn.close()
        return results, execution_times
        
    except Exception as e:
        print(f"Error getting streaming results: {e}")
        return None, None

def compare_results(batch_results, streaming_results, batch_times, streaming_times):
    print("\n=== Performance Comparison ===")
    print("\nExecution Times (seconds):")
    comparison_data = []
    for query in batch_times.keys():
        comparison_data.append([
            query,
            f"{batch_times[query]:.4f}",
            f"{streaming_times[query]:.4f}",
            f"{(streaming_times[query] - batch_times[query]):.4f}"
        ])
    print(tabulate(comparison_data, 
                  headers=['Query', 'Batch Time', 'Streaming Time', 'Difference'],
                  tablefmt='grid'))
    
    print("\n=== Results Comparison ===")
    for query in batch_results.keys():
        print(f"\n{query}:")
        print("\nBatch Results:")
        print(tabulate(batch_results[query], tablefmt='grid'))
        print("\nStreaming Results:")
        print(tabulate(streaming_results[query], tablefmt='grid'))
        
        # Check for discrepancies
        batch_set = set(str(row) for row in batch_results[query])
        streaming_set = set(str(row) for row in streaming_results[query])
        
        if batch_set == streaming_set:
            print("\nResults match perfectly!")
        else:
            print("\nDiscrepancies found between batch and streaming results")
            print("Differences in batch results:", batch_set - streaming_set)
            print("Differences in streaming results:", streaming_set - batch_set)

def main():
    # Initialize Spark
    spark = create_spark_session()
    
    # Get batch results
    print("Running batch processing...")
    batch_results, batch_times = get_batch_results(spark)
    
    # Get streaming results
    print("\nGetting streaming results...")
    streaming_results, streaming_times = get_streaming_results()
    
    # Compare results
    compare_results(batch_results, streaming_results, batch_times, streaming_times)
    
    # Stop Spark
    spark.stop()

if __name__ == "__main__":
    main() 