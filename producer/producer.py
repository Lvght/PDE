import os
import json
import time
from kafka import KafkaProducer, KafkaClient


def main():
    kafka_broker = os.getenv('KAFKA_BROKER')
    topic_name = os.getenv('TOPIC_NAME')
    data_dir = '/data'

    print('Waiting for Kafka to be ready...')
    time.sleep(10)
    print('Done waiting.')

    print('Creating the producer instance...')
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print('Producer instance created.')

    print('Trying to send messages...')
    json_file_path = os.path.join(data_dir, 'data.json')
    with open(json_file_path, 'r') as f:
        data = json.load(f)
    print('Data loaded from the JSON file.')

    # Assuming the JSON file contains a list of messages
    for message in data:
        producer.send(topic_name, message)
        print(f'Sent: {message}')
        time.sleep(1)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
