from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import time

# Kafka configuration
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "rfidmsg"
MININUM_RSSI = -400

print('Waiting for Kafka to be ready...')
time.sleep(30)
print('Done waiting.')

# Define the schema for the incoming data
reading_schema = StructType([
    StructField("forkliftid", IntegerType()),
    StructField("tabletid", IntegerType()),
    StructField("readerid", IntegerType()),
    StructField("record_start", TimestampType()),
    StructField("record_end", TimestampType()),
    StructField("read_timestamp", TimestampType()),
    StructField("battery_v_mean", DoubleType()),
    StructField("epc", StringType()),
    StructField("rssi_mean", DoubleType()),
    StructField("rps_mean", DoubleType()),
    StructField("read_count", IntegerType())
])

# Create a Spark session
spark = SparkSession \
    .builder \
    .appName("KafkaStreamProcessor") \
    .getOrCreate()

# Read the stream from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# Parse the Kafka message value to JSON and apply the schema
readings_df = kafka_df.select(from_json(col("value").cast("string"), reading_schema).alias("data")) \
    .select("data.*")

# Print the schema of the incoming data for verification
readings_df.printSchema()

# Write the filtered stream to the console (for debugging)
writing_df = readings_df \
    .filter(col("rssi_mean") > MININUM_RSSI) \
    .writeStream \
    .format("console") \
    .start()

# Wait for the streaming to finish
writing_df.awaitTermination()
