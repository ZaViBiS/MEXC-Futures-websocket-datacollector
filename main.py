import websockets
import threading
import asyncio
import json
import time


from db.db import DB
from shared.queue import q


URI = "wss://contract.mexc.com/edge"
SYMBOL = "LTC_USDT"
SUB_DEAL = {"method": "sub.deal", "param": {"symbol": SYMBOL}}
SUB_FUND = {"method": "sub.funding.rate", "param": {"symbol": SYMBOL}}
# SUB_DEPTH = {"method": "sub.depth", "param": {"symbol": SYMBOL}}

db = DB()


async def connect():
    async with websockets.connect(URI, ping_interval=None) as web:
        await web.send(json.dumps(SUB_DEAL))
        await web.send(json.dumps(SUB_FUND))
        # await web.send(json.dumps(SUB_DEPTH))
        start = 0

        while True:
            r = await web.recv()
            data = json.loads(r)
            print(data)
            q.put(data)
            # if data["channel"] == "push.deal":
            #     db.add(data["data"])
            # if data["channel"] == "push.funding.rate":
            #     db.add_rate(data)

            # ping-pong
            if time.time() - start > 10:
                await web.send(json.dumps({"method": "ping"}))
                start = time.time()


t = threading.Thread(target=db.loop)
t.start()


try:
    while True:
        asyncio.run(connect())
except Exception as e:
    print(e)
