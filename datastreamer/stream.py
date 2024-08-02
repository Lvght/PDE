import asyncio
import pandas as pd
import websockets
import time


async def stream_data(websocket, path):
    data = pd.read_csv('data.csv')

    for _, row in data.iterrows():
        await websocket.send(row.to_json())
        await asyncio.sleep(1)  # Stream each item every 1 second


async def main():
    async with websockets.serve(stream_data, "0.0.0.0", 8765):
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    asyncio.run(main())
