#!/usr/bin/env python3
# coding: utf-8
"""
    :author: mystertech, pk13055
    :brief: connect to binance live streaming data through websockets

"""
from enum import Enum
import os
import sys
from time import gmtime
from threading import Thread
from datetime import datetime
from decimal import Decimal

import asyncpg
from asyncio import Queue, get_event_loop
from binance.client import Client
from binance.websockets import BinanceSocketManager
from binance.exceptions import BinanceAPIException
from twisted.internet import reactor


last_price, mark_price = (Decimal('-1.'), Decimal('-1.')), Decimal('-1.')
vol = Decimal('0.')
new_candle = False
candles = [[]]
ohlc, ticks = Queue(), Queue()


class Stream(str, Enum):
    """Types of streams to fetch data from"""
    MARK_PRICE = "btcusdt@markPrice@1s"
    LAST_PRICE = "btcusdt@bookTicker"

    def __repr__(self):
        return self.value


class CandleMaker(Thread):
    def __init__(self, name):
        super().__init__()
        self.name = name

    def run(self):
        """Dynamically calculate candles"""
        global candles
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
                ohlc.put_nowait(candle)
                print(
                    f"{timestamp.tm_hour}:{timestamp.tm_min}\t::\tO:{o: .4f} H:{h: .4f} L:{l: .4f} C:{c: .4f} V:{v: .5f}")
                prev_timestamp = timestamp


async def dump_to_db():
    """Save candles to db"""
    # set delays for writing of chunks
    candle_delay = 1
    ticker_delay = 10
    _candles = []
    _ticks = []
    DATABASE_URI = os.getenv("DATABASE_URI",
                         'postgresql://postgres:postgres@localhost/test')
    pool = await asyncpg.create_pool(DATABASE_URI, timeout=60)
    while True:
        if not ticks.empty():
            data = await ticks.get()
            timestamp = datetime(*data['t'][:6])
            row = [timestamp, data['m'], data['a'], data['b'], data['v']]
            _ticks.append(row)
        if not ohlc.empty():
            data = await ohlc.get()
            timestamp = datetime(*data['t'][:6])
            row = [timestamp, data['o'], data['h'], data['l'], data['c'], data['v']]
            _candles.append(row)

        if len(_candles) > candle_delay:
            conn = await pool.acquire()
            await conn.executemany("""
                INSERT INTO ohlc(timestamp, open, high, low, close, vol)
                    VALUES($1, $2, $3, $4, $5, $6)
            """, _candles)
            _candles = list()
            await conn.close()
        if len(_ticks) > ticker_delay:
            conn = await pool.acquire()
            await conn.executemany("""
                INSERT INTO ticker(timestamp, mark, ask, bid, vol)
                    VALUES($1, $2, $3, $4, $5)
            """, _ticks)
            _ticks = list()
            await conn.close()


def stream_callback(msg: dict):
    """function processing the stream messages"""
    global last_price, mark_price, vol, candles
    timestamp = gmtime(msg['data']['E'] / 1000)
    if msg['stream'] == Stream.LAST_PRICE:
        last_price = Decimal(msg['data']['a']), Decimal(msg['data']['b'])
        vol += min(Decimal(msg['data']['B']), Decimal(msg['data']['A']))
    else:
        mark_price = Decimal(msg['data']['p'])
        ask, bid = last_price
        datapoint = {
            't': timestamp,
            'm': mark_price,
            'a': ask,
            'b': bid,
            'v': vol
        }
        ticks.put_nowait(datapoint)
        if timestamp.tm_sec:
            candles[-1].append(datapoint)
        else:
            candles.append([datapoint])
        vol = Decimal('0.')  # reset volume for the next second


async def main():
    API_KEY = os.getenv("BINANCE_API_KEY", None)
    API_SECRET = os.getenv("BINANCE_API_SECRET", None)
    client = Client(api_key=API_KEY, api_secret=API_SECRET)

    bm = BinanceSocketManager(client)
    candle_agent = CandleMaker("candle_maker")

    sockets = [
        bm.start_symbol_mark_price_socket(
            'BTCUSDT', stream_callback),  # mark price stream
        bm.start_symbol_ticker_futures_socket(
            'BTCUSDT', stream_callback),  # last price stream
    ]

    print(client.get_asset_balance("BTC"),
          client.get_asset_balance("USDT"), sep="\n")

    try:
        if all(sockets):
            bm.start()
            candle_agent.start()
            await dump_to_db()
        else:
            raise KeyboardInterrupt
    except KeyboardInterrupt:
        map(lambda socket: bm.stop_socket if socket else None, sockets)
        bm.close()
        candle_agent.join()
        reactor.stop()


if __name__ == "__main__":
    try:
        get_event_loop().run_until_complete(main())
    except BinanceAPIException as e:
        sys.stderr.write(f"[error] Binance: {e}\n")
