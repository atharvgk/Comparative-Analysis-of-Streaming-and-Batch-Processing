from db_utils import DatabaseManager
from tabulate import tabulate
from datetime import datetime

def load_streaming_metrics():
    try:
        with open('streaming_metrics.txt', 'r') as f:
            lines = f.readlines()
            if not lines:
                return None
            
            # Get the last batch of metrics
            last_batch = []
            current_batch = []
            for line in lines:
                if line.startswith('=== Streaming Metrics at'):
                    if current_batch:
                        last_batch = current_batch
                    current_batch = []
                current_batch.append(line)
            
            metrics = {
                'hashtags': {},
                'engagement': {},
                'languages': {},
                'total_tweets': 0,
                'total_engagement': 0
            }
            
            # Parse the last batch of metrics
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
            
            return metrics
    except FileNotFoundError:
        print("Warning: streaming_metrics.txt not found")
        return None
    except Exception as e:
        print(f"Error loading streaming metrics: {e}")
        return None

def get_batch_metrics(db):
    metrics = {
        'hashtags': {},
        'engagement': {},
        'languages': {},
        'total_tweets': 0,
        'total_engagement': 0
    }
    
    try:
        # Get total tweets
        db.cursor.execute("SELECT COUNT(*) FROM tweets")
        metrics['total_tweets'] = db.cursor.fetchone()[0]
        
        # Get engagement metrics
        db.cursor.execute("""
            SELECT username, 
                   SUM(reply_count + retweet_count + like_count + quote_count) as total
            FROM tweets
            GROUP BY username
            ORDER BY total DESC
            LIMIT 10
        """)
        for username, total in db.cursor.fetchall():
            metrics['engagement'][username] = total
            metrics['total_engagement'] += total
        
        # Get language distribution
        db.cursor.execute("""
            SELECT language, COUNT(*) as count
            FROM tweets
            GROUP BY language
            ORDER BY count DESC
            LIMIT 10
        """)
        for language, count in db.cursor.fetchall():
            metrics['languages'][language] = count
        
        return metrics
    except Exception as e:
        print(f"Error getting batch metrics: {e}")
        return metrics

def compare_metrics(streaming_metrics, batch_metrics):
    if not streaming_metrics or not batch_metrics:
        print("Error: Could not load metrics for comparison")
        return
    
    print("\n=== Metrics Comparison ===")
    print(f"Comparison Time: {datetime.now()}")
    
    # Compare total tweets
    print("\nTotal Tweets:")
    print(tabulate([
        ["Streaming", streaming_metrics['total_tweets']],
        ["Batch", batch_metrics['total_tweets']],
        ["Difference", streaming_metrics['total_tweets'] - batch_metrics['total_tweets']]
    ], headers=["Source", "Count"]))
    
    # Compare total engagement
    print("\nTotal Engagement:")
    print(tabulate([
        ["Streaming", streaming_metrics['total_engagement']],
        ["Batch", batch_metrics['total_engagement']],
        ["Difference", streaming_metrics['total_engagement'] - batch_metrics['total_engagement']]
    ], headers=["Source", "Count"]))
    
    # Compare top engaging users
    print("\nTop Engaging Users Comparison:")
    engagement_data = []
    all_users = set(list(streaming_metrics['engagement'].keys()) + 
                   list(batch_metrics['engagement'].keys()))
    for user in sorted(all_users,
                      key=lambda x: max(streaming_metrics['engagement'].get(x, 0),
                                      batch_metrics['engagement'].get(x, 0)),
                      reverse=True)[:10]:
        streaming_eng = streaming_metrics['engagement'].get(user, 0)
        batch_eng = batch_metrics['engagement'].get(user, 0)
        engagement_data.append([
            user,
            streaming_eng,
            batch_eng,
            streaming_eng - batch_eng
        ])
    print(tabulate(engagement_data,
                  headers=["User", "Streaming", "Batch", "Difference"]))
    
    # Compare language distribution
    print("\nLanguage Distribution Comparison:")
    language_data = []
    all_languages = set(list(streaming_metrics['languages'].keys()) + 
                       list(batch_metrics['languages'].keys()))
    for lang in sorted(all_languages,
                      key=lambda x: max(streaming_metrics['languages'].get(x, 0),
                                      batch_metrics['languages'].get(x, 0)),
                      reverse=True)[:10]:
        streaming_count = streaming_metrics['languages'].get(lang, 0)
        batch_count = batch_metrics['languages'].get(lang, 0)
        language_data.append([
            lang,
            streaming_count,
            batch_count,
            streaming_count - batch_count
        ])
    print(tabulate(language_data,
                  headers=["Language", "Streaming", "Batch", "Difference"]))

def main():
    # Load streaming metrics
    streaming_metrics = load_streaming_metrics()
    if not streaming_metrics:
        print("Error: Could not load streaming metrics")
        return
    
    # Get batch metrics
    db = DatabaseManager()
    db.connect()
    batch_metrics = get_batch_metrics(db)
    db.disconnect()
    
    # Compare metrics
    compare_metrics(streaming_metrics, batch_metrics)

if __name__ == "__main__":
    main() 