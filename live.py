#!/usr/bin/env python3
# coding: utf-8
"""
    :author: mystertech, pk13055
    :brief: connect to binance live streaming data through websockets

"""
from enum import Enum
import os
from pprint import pprint
import sys
from time import sleep, gmtime
from threading import Thread
from typing import Dict

from binance.client import Client
from binance.websockets import BinanceSocketManager
from binance.exceptions import BinanceAPIException
from twisted.internet import reactor


last_price, mark_price, vol = (-1., -1.), -1., 0.
new_candle = False
candles = [[]]


class Stream(str, Enum):
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
                    (timestamp := cur_candle[0]['timestamp']) != prev_timestamp:
                # OHLC calculation
                # NOTE: potentially calculate as `lambda tick: (tick['ask'] +
                # tick['bid'] / 2)` instead
                candle = {
                    't': timestamp,
                    'o': (o := cur_candle[0]['mark']),
                    'h': (h := max(map(lambda tick: tick['mark'], cur_candle))),
                    'l': (l := min(map(lambda tick: tick['mark'], cur_candle))),
                    'c': (c := cur_candle[-1]['mark']),
                    'v': (v := sum(map(lambda tick: tick['vol'], cur_candle))),
                }
                # TODO dump candle to db
                print(f"{timestamp.tm_hour}:{timestamp.tm_min}\t::\tO:{o: .4f} H:{h: .4f} L:{l: .4f} C:{c: .4f} V:{v: .5f}")
                prev_timestamp = timestamp


def stream_callback(msg: Dict):
    """function processing the stream messages"""
    global last_price, mark_price, vol, candles
    timestamp = gmtime(msg['data']['E'] / 1000)
    if msg['stream'] == Stream.LAST_PRICE:
        last_price = float(msg['data']['a']), float(msg['data']['b'])
        vol += min(float(msg['data']['B']), float(msg['data']['A']))
    else:
        mark_price = float(msg['data']['p'])
        ask, bid = last_price
        datapoint = {
            'timestamp': timestamp,
            'mark': mark_price,
            'ask': ask,
            'bid': bid,
            'vol': vol
        }
        if timestamp.tm_sec:
            candles[-1].append(datapoint)
        else:
            candles.append([datapoint])
        vol = 0.  # reset volume for the next second


def main():
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
        else:
            raise KeyboardInterrupt
    except KeyboardInterrupt:
        map(lambda socket: bm.stop_socket if socket else None, sockets)
        bm.close()
        candle_agent.join()
        reactor.stop()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except BinanceAPIException as e:
        sys.stderr.write(f"[error] Binance: {e}\n")