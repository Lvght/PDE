import asyncio
import json
import os
import threading
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import websockets

DATA_PROVIDER_HOST = "datastreamer"
DATA_PROVIDER_PORT = "8765"
URI = "ws://" + DATA_PROVIDER_HOST + ":" + DATA_PROVIDER_PORT
MININUM_RSSI = -400

PATH = "./data/"

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


# WebSocket client that writes messages to a file
async def websocket_client(uri, output_path):
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    async with websockets.connect(uri) as ws:
        try:
            while True:
                message = await ws.recv()
                data = json.loads(message)
                file_path = os.path.join(output_path, f"{int (time.time())}.json")
                with open(file_path, "a+") as f:
                    f.write(json.dumps(data) + "\n")
        finally:
            ws.close()


def spark_worker():
    spark = SparkSession \
        .builder \
        .appName("WebSocketProcessor") \
        .getOrCreate()

    socket_df = spark.readStream \
        .format("json") \
        .schema(reading_schema) \
        .load(PATH)


    writing_df = socket_df \
        .filter(col("rssi_mean") > MININUM_RSSI) \
        .writeStream \
        .format("console") \
        .start()

    writing_df.awaitTermination()

if __name__ == "__main__":
    ws_thread = threading.Thread(target = asyncio.run , args= (websocket_client(URI, PATH), ))
    ws_thread.start()

    spark_worker_thread = threading.Thread(target = spark_worker)
    spark_worker_thread.start()

    ws_thread.join()
    spark_worker_thread.join()
