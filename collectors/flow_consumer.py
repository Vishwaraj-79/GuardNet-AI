from kafka import KafkaConsumer
import json

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'raw-ue-flows'

def consume_flows():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    
    print(f"🔵 Listening to Kafka topic: {TOPIC_NAME}")
    print("🔵 Press Ctrl+C to stop\n")
    
    count = 0
    try:
        for message in consumer:
            flow = message.value
            count += 1
            status = "⚠️ MALICIOUS" if flow.get('is_malicious', False) else "✅ Normal"
            print(f"{count:5d} | {flow['timestamp'][:19]} | {flow['slice_type']:8s} | {flow['ue_ip']:15s} | {status}")
            
    except KeyboardInterrupt:
        print("\n🔴 Stopping...")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_flows()