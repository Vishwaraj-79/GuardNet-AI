import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'raw-ue-flows'

# 5G Slice Types
SLICE_TYPES = ['eMBB', 'URLLC', 'mMTC', 'Control', 'Management']

# Network Functions
NF_TYPES = ['AMF', 'SMF', 'UPF', 'NEF', 'NWDAF', 'gNB']

class FiveGFlowSimulator:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.ue_count = 0
        self.ue_ip_pool = []
        
        # Generate some UEs
        for _ in range(50):
            self.ue_ip_pool.append(fake.ipv4())
    
    def generate_flow(self):
        """Generate a single 5G network flow"""
        ue_ip = random.choice(self.ue_ip_pool)
        slice_type = random.choice(SLICE_TYPES)
        nf_type = random.choice(NF_TYPES)
        
        # Adjust traffic patterns based on slice type
        if slice_type == 'URLLC':
            packet_count = random.randint(10, 100)
            byte_count = packet_count * random.randint(40, 100)
            duration = random.uniform(0.001, 0.1)
        elif slice_type == 'eMBB':
            packet_count = random.randint(100, 10000)
            byte_count = packet_count * random.randint(100, 1500)
            duration = random.uniform(0.5, 60.0)
        else:  # mMTC
            packet_count = random.randint(1, 20)
            byte_count = packet_count * random.randint(50, 200)
            duration = random.uniform(0.1, 5.0)
        
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'ue_ip': ue_ip,
            'slice_type': slice_type,
            'nf_type': nf_type,
            'dst_ip': fake.ipv4(),
            'src_port': random.randint(1024, 65535),
            'dst_port': random.choice([80, 443, 53, 123, 5060, 22]),
            'protocol': random.choice(['TCP', 'UDP', 'ICMP']),
            'packet_count': packet_count,
            'byte_count': byte_count,
            'duration': round(duration, 3),
            'qci': random.randint(1, 9),
            'is_malicious': False
        }
    
    def run(self, interval=0.5):
        """Continuously generate and send flows"""
        print(f"🔵 Streaming 5G flows to Kafka topic: {TOPIC_NAME}")
        print(f"🔵 Press Ctrl+C to stop\n")
        
        try:
            while True:
                flow = self.generate_flow()
                self.producer.send(TOPIC_NAME, value=flow)
                
                # Add occasional malicious traffic (10% probability)
                if random.random() < 0.1:  # 10% chance
                    flow = self.generate_flow()
                    flow['is_malicious'] = True
                    flow['packet_count'] *= random.randint(5, 20)  # DDoS spike
                    self.producer.send(TOPIC_NAME, value=flow)
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n🔴 Stopping...")
        finally:
            self.producer.close()

if __name__ == "__main__":
    simulator = FiveGFlowSimulator()
    simulator.run(interval=0.2)  # 5 flows per second