import os
import json
import time
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable

def wait_for_broker(kafka_broker, timeout=30):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)
            admin_client.list_topics()
            return True
        except NoBrokersAvailable:
            print("Kafka broker not available, retrying...")
            time.sleep(1)
    return False

def main():
    kafka_broker = os.getenv('KAFKA_BROKER')
    topic_name = os.getenv('TOPIC_NAME')
    data_dir = '/data'

    print("Trying to Connect to Kafka Broker at", kafka_broker)

    if not wait_for_broker(kafka_broker):
        print(f"Could not connect to Kafka Broker. Exiting")
        return

    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    json_file_path = os.path.join(data_dir, 'data.json')
    with open(json_file_path, 'r') as f:
        data = json.load(f)

    # Assuming the JSON file contains a list of messages
    for message in data:
        producer.send(topic_name, message)
        print(f'Sent: {message}')
        time.sleep(1)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()
