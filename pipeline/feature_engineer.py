import json
import time
import pandas as pd
import numpy as np
from collections import defaultdict
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
import threading
import queue

# ====== FIX: Convert numpy types to Python types ======
def convert_to_serializable(obj):
    """Convert numpy types to Python types for JSON serialization"""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(item) for item in obj]
    else:
        return obj
# ======================================================

class FeatureEngineer:
    def __init__(self, window_size=10):
        """
        window_size: seconds to aggregate flows
        """
        self.window_size = window_size
        self.flow_buffer = []
        self.feature_queue = queue.Queue()
        
        # Kafka setup
        self.consumer = KafkaConsumer(
            'raw-ue-flows',
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.last_window_time = datetime.now()
        self.running = False
    
    def extract_features_from_window(self, flows):
        """Extract features from a window of flows"""
        if not flows:
            return None
        
        df = pd.DataFrame(flows)
        
        # Basic statistics per slice
        features = {}
        
        for slice_type in df['slice_type'].unique():
            slice_df = df[df['slice_type'] == slice_type]
            
            # Per-slice features
            slice_features = {
                'slice_type': slice_type,
                'window_start': self.last_window_time.isoformat(),
                'window_end': datetime.now().isoformat(),
                'flow_count': int(len(slice_df)),
                'total_packets': int(slice_df['packet_count'].sum()),
                'total_bytes': int(slice_df['byte_count'].sum()),
                'avg_duration': float(slice_df['duration'].mean()),
                'std_duration': float(slice_df['duration'].std()) if len(slice_df) > 1 else 0.0,
                'packet_rate': float(slice_df['packet_count'].sum() / self.window_size),
                'byte_rate': float(slice_df['byte_count'].sum() / self.window_size),
                'unique_ues': int(slice_df['ue_ip'].nunique()),
                'unique_nfs': int(slice_df['nf_type'].nunique()),
                'avg_qci': float(slice_df['qci'].mean()),
                'malicious_count': int(slice_df[slice_df.get('is_malicious', False) == True].shape[0]),
                'protocol_tcp_ratio': float((slice_df['protocol'] == 'TCP').mean()),
                'protocol_udp_ratio': float((slice_df['protocol'] == 'UDP').mean()),
                'protocol_icmp_ratio': float((slice_df['protocol'] == 'ICMP').mean()),
            }
            
            # Add per-slice traffic patterns
            if slice_type == 'URLLC':
                slice_features['latency_sensitive'] = 1
                slice_features['throughput_gbps'] = float(slice_features['byte_rate'] / 1e9)
            elif slice_type == 'eMBB':
                slice_features['throughput_mbps'] = float(slice_features['byte_rate'] / 1e6)
                slice_features['large_packet_ratio'] = float((slice_df['packet_count'] > 1000).mean())
            elif slice_type == 'mMTC':
                slice_features['device_density'] = float(slice_features['unique_ues'] / self.window_size)
                slice_features['small_packet_ratio'] = float((slice_df['packet_count'] < 10).mean())
            
            # Normalize features (Z-score approximation)
            for key in ['flow_count', 'total_packets', 'total_bytes', 'packet_rate', 'byte_rate']:
                if key in slice_features:
                    slice_features[f'{key}_norm'] = float(slice_features[key] / (slice_features[key] + 1))
            
            features[f'slice_{slice_type}'] = slice_features
        
        return features
    
    def process_stream(self):
        """Main processing loop"""
        print(f"🔵 Feature Engineer started (window={self.window_size}s)")
        print("🔵 Processing flows from Kafka...\n")
        
        self.running = True
        
        try:
            for message in self.consumer:
                flow = message.value
                current_time = datetime.now()
                
                # Add flow to buffer
                self.flow_buffer.append(flow)
                
                # Check if window is complete
                if (current_time - self.last_window_time).total_seconds() >= self.window_size:
                    if self.flow_buffer:
                        # Extract features
                        features = self.extract_features_from_window(self.flow_buffer)
                        
                        if features:
                            # FIX: Convert numpy types to Python types
                            features_serializable = convert_to_serializable(features)
                            self.producer.send('featured-flows', value=features_serializable)
                            
                            # Print sample features
                            for slice_name, slice_features in features.items():
                                print(f"📊 {slice_name}: flows={slice_features['flow_count']}, "
                                      f"packets={slice_features['total_packets']}, "
                                      f"rate={slice_features['packet_rate']:.1f}/s")
                            
                            self.feature_queue.put(features)
                        
                        # Reset buffer
                        self.flow_buffer = []
                        self.last_window_time = current_time
                    
        except KeyboardInterrupt:
            print("\n🔴 Stopping feature engineer...")
        finally:
            self.consumer.close()
            self.producer.close()
            self.running = False
    
    def run(self):
        """Start the feature engineer"""
        self.process_stream()
    
    def get_latest_features(self):
        """Get latest features from queue"""
        try:
            return self.feature_queue.get_nowait()
        except queue.Empty:
            return None

if __name__ == "__main__":
    engineer = FeatureEngineer(window_size=10)
    engineer.run()