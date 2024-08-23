from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import time

# Kafka configuration
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "rfidmsg"
MININUM_RSSI = -60

print('Waiting for Kafka to be ready...')
time.sleep(30)
print('Done waiting.')

# Define the schema for the incoming data
readings_schema = StructType([
    StructField("battery_v_mean", StringType(), True),
    StructField("epc", StringType(), True),
    StructField("rssi_mean", StringType(), True),
    StructField("read_count", StringType(), True),
    StructField("rps_mean", StringType(), True)
])

# Define the schema for 'data'
data_schema = StructType([
    StructField("readerid", StringType(), True),
    StructField("readings", ArrayType(readings_schema), True)
])

# Define the main schema
json_schema = StructType([
    StructField("read_timestamp", StringType(), True),
    StructField("data", ArrayType(data_schema), True)
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

parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), json_schema).alias("data"))

# Explode the nested arrays to flatten the structure
flattened_df = parsed_df.selectExpr("data.read_timestamp", "explode(data.data) as data_exploded") \
    .selectExpr("read_timestamp", "data_exploded.readerid", "explode(data_exploded.readings) as readings_exploded") \
    .selectExpr("read_timestamp", "readerid", "readings_exploded.*")

# Filter the DataFrame based on the rssi_mean threshold
filtered_df = flattened_df.filter(col("rssi_mean").cast("double") > MININUM_RSSI)

# Write the filtered DataFrame to another DataFrame or sink
query = filtered_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()