from asyncio import Queue
from collections import deque
from datetime import datetime
from enum import Enum
import os
from threading import Thread

import asyncpg


MAX_LEN = 100
candles = deque([[]], maxlen=MAX_LEN)
ohlc, ticks = Queue(), Queue()


class Stream(str, Enum):
    """Types of streams to fetch data from"""
    MARK_PRICE = "btcusdt@markPrice@1s"
    LAST_PRICE = "btcusdt@bookTicker"

    def __repr__(self):
        return self.value


class CandleMaker(Thread):
    def __init__(self):
        super().__init__()

    def run(self):
        """Dynamically calculate candles"""
        global candles, ohlc
        prev_timestamp = None
        while True:
            if len(cur_candle := candles[-1]) == 60 and \
                    (timestamp := cur_candle[0]['t']) != prev_timestamp:
                # OHLC calculation
                # NOTE: potentially calculate as `lambda tick: (tick['ask'] +
                # tick['bid'] / 2)` instead
                candle = {
                    't': timestamp,
                    'o': (o := cur_candle[0]['m']),
                    'h': (h := max(map(lambda tick: tick['m'], cur_candle))),
                    'l': (l := min(map(lambda tick: tick['m'], cur_candle))),
                    'c': (c := cur_candle[-1]['m']),
                    'v': (v := sum(map(lambda tick: tick['v'], cur_candle))),
                }
                # TODO: publish the message to queue here
                ohlc.put_nowait(candle)
                print(
                    f"{timestamp.tm_hour}:{timestamp.tm_min}\t::\tO:{o: .4f} H:{h: .4f} L:{l: .4f} C:{c: .4f} V:{v: .5f}")
                prev_timestamp = timestamp

class CandleDumper(Thread):
    def __init__(self, candle_delay: int=1, ticker_delay: int=10):
        super().__init__()
        # set delays for writing of chunks
        self.candle_delay = 1
        self.ticker_delay = 10
        self._candles = deque(maxlen=candle_delay)
        self._ticks = deque(maxlen=ticker_delay)
        self.DATABASE_URI = os.getenv("DATABASE_URI",
                            'postgresql://postgres:postgres@localhost/test')

    async def run(self):
        self.pool = await asyncpg.create_pool(self.DATABASE_URI, timeout=60)
        await self.dump_to_db()

    async def dump_to_db(self):
        """Save candles to db"""
        global ticks, ohlc
        while True:
                if not ticks.empty():
                    data = await ticks.get()
                    timestamp = datetime(*data['t'][:6])
                    row = [timestamp, data['m'], data['a'], data['b'], data['v']]
                    self._ticks.append(row)
                if not ohlc.empty():
                    data = await ohlc.get()
                    timestamp = datetime(*data['t'][:6])
                    row = [timestamp, data['o'], data['h'], data['l'], data['c'], data['v']]
                    self._candles.append(row)

                if len(self._candles) >= self.candle_delay:
                    conn = await self.pool.acquire()
                    await conn.executemany("""
                        INSERT INTO ohlc(timestamp, open, high, low, close, vol)
                            VALUES($1, $2, $3, $4, $5, $6)
                    """, self._candles)
                    self._candles = list()
                    await conn.close()
                if len(self._ticks) >= self.ticker_delay:
                    conn = await self.pool.acquire()
                    await conn.executemany("""
                        INSERT INTO ticker(timestamp, mark, ask, bid, vol)
                            VALUES($1, $2, $3, $4, $5)
                    """, self._ticks)
                    self._ticks = list()
                    await conn.close()

