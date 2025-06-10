import websockets
import threading
import asyncio
import logging
import json
import time


from db.db import DB
from shared.queue import q
from shared.config import SYMBOLS
from shared.logging_setup import setup_logging


URI = "wss://contract.mexc.com/edge"

setup_logging()
log = logging.getLogger(__name__)


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
                        log.debug(data)
                        q.put(data)

                        # ping-pong
                        if time.time() - start > 10:
                            loop = asyncio.get_event_loop()
                            loop.create_task(web.send(json.dumps({"method": "ping"})))
                            start = time.time()
                    except Exception as e:
                        log.error(f"виникла помилка під час отримання даних: {e}")
                        break
        except Exception as e:
            log.error(f"виникла помилка під час підключення: {e}")


t = threading.Thread(target=db.loop)
t.start()

asyncio.run(connect())
