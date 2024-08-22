# datastreamer/kafka_producer.py
import pandas as pd
from kafka import KafkaProducer
import time

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers='kafka:9092')

topic = 'csv_topic'

csv_file = 'data.csv'
df = pd.read_csv(csv_file)

for _, row in df.iterrows():
    producer.send(topic, row.to_json().encode('utf-8'))
    time.sleep(1)  # Adjust this as needed for your rate

producer.flush()
producer.close()
