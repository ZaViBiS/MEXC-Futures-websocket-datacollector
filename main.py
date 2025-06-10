import websockets
import threading
import asyncio
import json
import time


from db.db import DB
from shared.queue import q
from shared.config import SYMBOLS


URI = "wss://contract.mexc.com/edge"


def methods(symbol: str):
    return {"method": "sub.deal", "param": {"symbol": symbol}}, {
        "method": "sub.funding.rate",
        "param": {"symbol": symbol},
    }


db = DB()


async def connect():
    while True:
        try:
            async with websockets.connect(URI, ping_interval=None) as web:
                for symbol in SYMBOLS:
                    sub_deal, sub_fund = methods(symbol)
                    await web.send(json.dumps(sub_deal))
                    await web.send(json.dumps(sub_fund))
                # await web.send(json.dumps(SUB_DEPTH))
                start = 0

                while True:
                    try:
                        r = await web.recv()
                        data = json.loads(r)
                        print(data)
                        q.put(data)

                        # ping-pong
                        if time.time() - start > 10:
                            await web.send(json.dumps({"method": "ping"}))
                            start = time.time()
                    except Exception as e:
                        print(f"виникла помилка під час отримання даних: {e}")
        except Exception as e:
            print(f"виникла помилка під час підключення: {e}")


t = threading.Thread(target=db.loop)
t.start()

asyncio.run(connect())
