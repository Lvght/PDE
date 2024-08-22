import os
import json
from kafka import KafkaProducer


def main():
    kafka_broker = os.getenv('KAFKA_BROKER')
    topic_name = os.getenv('TOPIC_NAME')
    data_dir = '/data'

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
        sleep(1)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()
