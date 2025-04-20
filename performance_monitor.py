import time
import psutil
import os
from datetime import datetime
import json

class PerformanceMonitor:
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.metrics = {
            'execution_time': 0,
            'cpu_usage': [],
            'memory_usage': [],
            'disk_io': [],
            'network_io': []
        }
        self.process = psutil.Process(os.getpid())
    
    def start_monitoring(self):
        self.start_time = time.time()
        self.metrics = {
            'execution_time': 0,
            'cpu_usage': [],
            'memory_usage': [],
            'disk_io': [],
            'network_io': []
        }
    
    def record_metrics(self):
        # Record CPU usage
        cpu_percent = self.process.cpu_percent()
        self.metrics['cpu_usage'].append({
            'timestamp': datetime.now().isoformat(),
            'value': cpu_percent
        })
        
        # Record memory usage
        memory_info = self.process.memory_info()
        self.metrics['memory_usage'].append({
            'timestamp': datetime.now().isoformat(),
            'value': memory_info.rss / 1024 / 1024  # Convert to MB
        })
        
        # Record disk I/O
        disk_io = psutil.disk_io_counters()
        self.metrics['disk_io'].append({
            'timestamp': datetime.now().isoformat(),
            'read_bytes': disk_io.read_bytes,
            'write_bytes': disk_io.write_bytes
        })
        
        # Record network I/O
        net_io = psutil.net_io_counters()
        self.metrics['network_io'].append({
            'timestamp': datetime.now().isoformat(),
            'bytes_sent': net_io.bytes_sent,
            'bytes_recv': net_io.bytes_recv
        })
    
    def stop_monitoring(self):
        self.end_time = time.time()
        self.metrics['execution_time'] = self.end_time - self.start_time
    
    def save_metrics(self, filename):
        with open(filename, 'w') as f:
            json.dump(self.metrics, f, indent=2)
    
    def get_summary(self):
        return {
            'execution_time': self.metrics['execution_time'],
            'avg_cpu_usage': sum(x['value'] for x in self.metrics['cpu_usage']) / len(self.metrics['cpu_usage']),
            'max_memory_usage': max(x['value'] for x in self.metrics['memory_usage']),
            'total_disk_read': self.metrics['disk_io'][-1]['read_bytes'] - self.metrics['disk_io'][0]['read_bytes'],
            'total_disk_write': self.metrics['disk_io'][-1]['write_bytes'] - self.metrics['disk_io'][0]['write_bytes'],
            'total_network_sent': self.metrics['network_io'][-1]['bytes_sent'] - self.metrics['network_io'][0]['bytes_sent'],
            'total_network_recv': self.metrics['network_io'][-1]['bytes_recv'] - self.metrics['network_io'][0]['bytes_recv']
        } 