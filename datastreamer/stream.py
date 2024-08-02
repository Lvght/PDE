import asyncio
import pandas as pd
import websockets
import time


async def stream_data(websocket, path):
    data = pd.read_csv('data.csv')
    print("Data stream started")
    for _, row in data.iterrows():
        try:
            print(f'Seding data {row.to_json()}')
            await websocket.send(row.to_json())
            await asyncio.sleep(1)  # Stream each item every 1 second
        except websockets.exceptions.ConnectionClosedError as e:
            print(f'Connection closed: {e}')
        except Exception as e:
            print(f'Error: {e}')

    print('Data stream completed')


async def run_websocket():
    '''
    Runs the program indefinitely.
    '''
    print('Starting to work.')
    await websockets.serve(stream_data, "0.0.0.0", 8765)

    #Holds the process
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        print('Shutting down. Received a asyncio.CancelledError')

    print('Done.')

if __name__ == '__main__':
    print('Ok, starting up...')
    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(run_websocket())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
