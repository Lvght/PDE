from collections import defaultdict
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import json

reading_schema = {
    "forkliftid": np.int32,
    "tabletid": np.int32,
    "readerid": np.int32,
    "record_start": str,
    "record_end": str,
    "read_timestamp": str,
    "battery_v_mean": np.float32,
    "epc": str,
    "rssi_mean": np.float32,
    "rps_mean": np.float32,
    "read_count": np.int32
}

print(f'Starting to read csv: {datetime.now(timezone.utc)}')
df = pd.read_csv(
    filepath_or_buffer='data.csv',
    sep= ',',
    header=0,
    names=["forkliftid", "tabletid", "readerid", "record_start", "record_end", "read_timestamp", "battery_v_mean", "epc", "rssi_mean", "rps_mean", "read_count"],
    index_col=False,
    dtype= reading_schema,
    usecols=["readerid","read_timestamp", "battery_v_mean", "epc", "rssi_mean", "rps_mean", "read_count"],
    parse_dates= ["read_timestamp"],
    date_format='%Y-%m-%d %H:%M:%S',
    low_memory=True
)

print(f'Filtering out NOTAG_EPC: {datetime.now(timezone.utc)}')
df = df[df["epc"] != "NOTAG_EPC"]

print(f'Trunc read_timestamp: {datetime.now(timezone.utc)}')
df['read_timestamp'] = pd.to_datetime(df['read_timestamp']).dt.floor('s')

print(f'Grouping data: {datetime.now(timezone.utc)}')
grouped_data = df.groupby(['read_timestamp', 'readerid'])

json_data_dict = defaultdict(lambda: {"read_timestamp": None, "data": []})

print(f'Creating json: {datetime.now(timezone.utc)}')
for (timestamp, readerid), group in grouped_data:
    readings_formatted = [
        {
            "battery_v_mean": str(reading['battery_v_mean']),
            "epc": str(reading['epc']),
            "rssi_mean": str(reading['rssi_mean']),
            "read_count": str(reading['read_count']),
            "rps_mean": str(reading['rps_mean'])
        }
        for _, reading in group.iterrows()
    ]
    timestamp_str = str(timestamp)
    
    # Ensure the timestamp entry exists in the dictionary
    if json_data_dict[timestamp_str]["read_timestamp"] is None:
        json_data_dict[timestamp_str]["read_timestamp"] = timestamp_str
    
    # Append the reader data
    json_data_dict[timestamp_str]["data"].append({
        "readerid": str(readerid),
        "readings": readings_formatted
    })

json_data = list(json_data_dict.values())

print(f'writing_json: {datetime.now(timezone.utc)}')
with open(f'output_file_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")}.json', 'w+') as json_file:
    json.dump(json_data, json_file, indent=4)

print(f'finishing work: {datetime.now(timezone.utc)}')