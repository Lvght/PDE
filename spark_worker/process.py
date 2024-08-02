from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

DATA_PROVIDER_HOST = "datastreamer"
DATA_PROVIDER_PORT = 8765
MININUM_RSSI = -400

# Based on `data.json`
schema = StructType([
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

spark = SparkSession \
    .builder \
    .appName("WebSocketProcessor") \
    .getOrCreate()

socket_df = spark.readStream \
    .format("socket") \
    .option("host", DATA_PROVIDER_HOST) \
    .option("port", DATA_PROVIDER_PORT) \
    .load()

socket_df.printSchema()

query = socket_df \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .where(col("data.rssi_mean") > MININUM_RSSI) \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
