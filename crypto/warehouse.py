import asyncio
from asyncio import Queue
from collections import deque
from datetime import datetime
from enum import Enum
import json
import os

from aio_pika import connect, IncomingMessage, ExchangeType
import asyncpg

from utils.encoder import EnhancedJSONDecoder


class Warehouse:
    """Store and catalog incoming ticker information to database"""

    def __init__(self, loop: asyncio.AbstractEventLoop, candle_delay: int = 3, ticker_delay: int = 10):
        # fetch connection URIs from env
        self.DATABASE_URI = os.getenv("DATABASE_URI",
                                      'postgresql://postgres:postgres@localhost/test')
        self.RABBIT_URI = os.getenv(
            "RABBIT_URI", "amqp://guest:guest@localhost/")
        self.loop = loop

        # set delays for writing of chunks
        self.candle_delay = candle_delay
        self.ticker_delay = ticker_delay
        # create data structure for temp data storage
        self._candles = deque(maxlen=candle_delay)
        self._ticks = deque(maxlen=ticker_delay)
        self.ohlc, self.ticks = Queue(), Queue()

        # set the topics to listen and dump
        # NOTE: expand on this as data size increases
        self.topics = [
            "crypto.futures.tick.btcusdt",
            "crypto.futures.ohlc.btcusdt"
        ]

    async def run(self):
        """Intialize and start the queue listeners and dumper"""
        self.pool = await asyncpg.create_pool(self.DATABASE_URI, timeout=60)

        # Creating a channel
        self.connection = await connect(self.RABBIT_URI, loop=self.loop)
        channel = await self.connection.channel()
        await channel.set_qos(prefetch_count=1)

        self.exchange = await channel.declare_exchange(
            "tickers", ExchangeType.TOPIC
        )

        # set up relevant queues for topics
        for topic in self.topics:
            queue = await channel.declare_queue()
            await queue.bind(self.exchange, topic)
            await queue.consume(self.on_message)

        self.loop.create_task(self.dump_to_db())

    async def on_message(self, message: IncomingMessage):
        """Process the message as it is delivered"""
        async with message.process():
            # TODO: load data into relevant queue depending on
            data = json.loads(message.body, cls=EnhancedJSONDecoder)
            if 'tick' in message.routing_key:
                self.ticks.put_nowait(data)
            elif 'ohlc' in message.routing_key:
                if (isinstance(data, list)):
                    [self.ohlc.put_nowait(datapoint) for datapoint in data]
                else:
                    self.ohlc.put_nowait(data)

    async def dump_to_db(self):
        """Save candles to db"""
        while True:
            await asyncio.sleep(0)
            if not self.ticks.empty():
                data = await self.ticks.get()
                row = [data['t'], data['m'], data['a'], data['b'], data['v']]
                self._ticks.append(row)
            if not self.ohlc.empty():
                data = await self.ohlc.get()
                row = [
                    timestamp := data['t'],
                    o := data['o'],
                    h := data['h'],
                    l := data['l'],
                    c := data['c'],
                    v := data['v']
                ]
                # print(
                #     f"{timestamp.hour}:{timestamp.minute}\t::\tO:{o: .4f} H:{h: .4f} L:{l: .4f} C:{c: .4f} V:{v: .5f}")
                self._candles.append(row)

            if len(self._candles) >= self.candle_delay:
                async with self.pool.acquire() as conn:
                    await conn.executemany("""
                            INSERT INTO ohlc(timestamp, open, high, low, close, vol)
                                VALUES($1, $2, $3, $4, $5, $6)
                        """, self._candles)
                    self._candles = list()
            if len(self._ticks) >= self.ticker_delay:
                async with self.pool.acquire() as conn:
                    await conn.executemany("""
                            INSERT INTO ticker(timestamp, mark, ask, bid, vol)
                                VALUES($1, $2, $3, $4, $5)
                        """, self._ticks)
                    self._ticks = list()
