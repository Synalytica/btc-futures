#!/usr/bin/env python3
# coding: utf-8
"""
    :author: mystertech, pk13055
    :brief: connect to binance live streaming data through websockets

"""
from enum import Enum
import os
import sys
from time import sleep
from typing import Dict

from binance.client import Client
from binance.websockets import BinanceSocketManager
from binance.exceptions import BinanceAPIException
from twisted.internet import reactor


last_price, mark_price = -1., -1.


class Stream(str, Enum):
    MARK_PRICE = "btcusdt@markPrice@1s"
    LAST_PRICE = "btcusdt@bookTicker"

    def __repr__(self):
        return self.value


def stream_callback(msg: Dict):
    """function processing the stream messages"""
    global last_price, mark_price
    timestamp = msg['data']['E']
    if msg['stream'] == Stream.LAST_PRICE:
        last_price = (float(msg['data']['a']) + float(msg['data']['b'])) / 2
    else:
        mark_price = float(msg['data']['p'])
    print(f"{timestamp}\tL:{last_price: .4f}\t|\tM:{mark_price: .4f}", end="\r", flush=True)
    # TODO start adding indicator calculation


def main():
    API_KEY = os.getenv("BINANCE_API_KEY", None)
    API_SECRET = os.getenv("BINANCE_API_SECRET", None)
    client = Client(api_key=API_KEY, api_secret=API_SECRET)

    bm = BinanceSocketManager(client)
    sockets = [
        bm.start_symbol_mark_price_socket('BTCUSDT', stream_callback),  # mark price stream
        bm.start_symbol_ticker_futures_socket('BTCUSDT', stream_callback),  # last price stream
    ]

    print(client.get_asset_balance("BTC"),
          client.get_asset_balance("USDT"), sep="\n")

    try:
        if all(sockets):
            bm.start()
        else:
            raise KeyboardInterrupt
    except KeyboardInterrupt:
        map(lambda socket: bm.stop_socket if socket else None, sockets)
        bm.close()
        reactor.stop()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except BinanceAPIException as e:
        sys.stderr.write(f"[error] Binance: {e}\n")

