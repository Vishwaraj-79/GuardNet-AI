import json
import psycopg2
from datetime import datetime
from kafka import KafkaConsumer

class FeatureStore:
    def __init__(self, db_name='guardnet', user='vikexx', password='vikexx', host='localhost'):
        """Initialize PostgreSQL connection"""
        self.conn = None
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        
        self.init_db()
        self.init_kafka()
    
    def init_db(self):
        """Create database and table if not exists"""
        try:
            # Connect to default postgres database
            conn = psycopg2.connect(dbname='postgres', user=self.user, password=self.password, host=self.host)
            conn.autocommit = True
            cur = conn.cursor()
            
            # Create database if not exists
            cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.db_name}'")
            exists = cur.fetchone()
            if not exists:
                cur.execute(f"CREATE DATABASE {self.db_name}")
                print(f"✅ Database '{self.db_name}' created")
            
            cur.close()
            conn.close()
            
            # Connect to our database
            self.conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host
            )
            
            # Create table
            cur = self.conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS slice_features (
                    id SERIAL PRIMARY KEY,
                    slice_type VARCHAR(20),
                    window_start TIMESTAMP,
                    window_end TIMESTAMP,
                    flow_count INTEGER,
                    total_packets INTEGER,
                    total_bytes INTEGER,
                    avg_duration FLOAT,
                    std_duration FLOAT,
                    packet_rate FLOAT,
                    byte_rate FLOAT,
                    unique_ues INTEGER,
                    unique_nfs INTEGER,
                    avg_qci FLOAT,
                    malicious_count INTEGER,
                    protocol_tcp_ratio FLOAT,
                    protocol_udp_ratio FLOAT,
                    protocol_icmp_ratio FLOAT,
                    is_anomaly BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.commit()
            cur.close()
            print(f"✅ Table 'slice_features' ready")
            
        except Exception as e:
            print(f"⚠️ Database error: {e}")
            print("   Installing PostgreSQL: sudo apt install postgresql postgresql-contrib -y")
            print("   Then: sudo -u postgres createdb guardnet")
            self.conn = None
    
    def init_kafka(self):
        """Setup Kafka consumer for features"""
        self.consumer = KafkaConsumer(
            'featured-flows',
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest'
        )
    
    def store_features(self):
        """Consume features and store in database"""
        if not self.conn:
            print("❌ No database connection")
            return
        
        print(f"🔵 Storing features to PostgreSQL...")
        
        try:
            for message in self.consumer:
                features = message.value
                cur = self.conn.cursor()
                
                for slice_name, sf in features.items():
                    slice_type = sf.get('slice_type', slice_name.replace('slice_', ''))
                    
                    cur.execute("""
                        INSERT INTO slice_features (
                            slice_type, window_start, window_end, flow_count,
                            total_packets, total_bytes, avg_duration, std_duration,
                            packet_rate, byte_rate, unique_ues, unique_nfs,
                            avg_qci, malicious_count, protocol_tcp_ratio,
                            protocol_udp_ratio, protocol_icmp_ratio
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        slice_type,
                        sf.get('window_start'),
                        sf.get('window_end'),
                        sf.get('flow_count', 0),
                        sf.get('total_packets', 0),
                        sf.get('total_bytes', 0),
                        sf.get('avg_duration', 0),
                        sf.get('std_duration', 0),
                        sf.get('packet_rate', 0),
                        sf.get('byte_rate', 0),
                        sf.get('unique_ues', 0),
                        sf.get('unique_nfs', 0),
                        sf.get('avg_qci', 0),
                        sf.get('malicious_count', 0),
                        sf.get('protocol_tcp_ratio', 0),
                        sf.get('protocol_udp_ratio', 0),
                        sf.get('protocol_icmp_ratio', 0)
                    ))
                
                self.conn.commit()
                cur.close()
                print(f"✅ Stored features at {datetime.now().strftime('%H:%M:%S')}")
                
        except KeyboardInterrupt:
            print("\n🔴 Stopping feature store...")
        finally:
            if self.conn:
                self.conn.close()

if __name__ == "__main__":
    # Install psycopg2 first: pip install psycopg2-binary
    store = FeatureStore()
    store.store_features()