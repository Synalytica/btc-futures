#!/usr/bin/env python3
# coding: utf-8
"""
    :author: mystertech, pk13055
    :brief: connect to binance live streaming data through websockets

"""
import asyncio
from collections import deque
from decimal import Decimal
from datetime import datetime
import json
import os
from typing import Tuple

from aio_pika import connect, Message, DeliveryMode, ExchangeType
from binance.client import Client
from binance.websockets import BinanceSocketManager
from twisted.internet import reactor

from utils.encoder import EnhancedJSONEncoder
from utils.enums import StreamType


class Stream:
    """Engine to run the streaming functionality"""

    def __init__(self, loop: asyncio.AbstractEventLoop, asset: Tuple[str, str], API_KEY: str, API_SECRET: str):
        """Initialize the Binance API connection for a given asset

        :Params:
            - asset: [Quote, Base], eg: ("BTC", "USDT") (must be all caps)
            - API_KEY: binance api key
            - API_SECRET: binance api secret
        """
        self.loop = loop

        MAX_LEN = 100
        self.candles = deque([[]], maxlen=MAX_LEN)

        self.quote, self.base = map(str.upper, asset)
        self.currency = f"{self.quote}{self.base}"

        self.last_price, self.mark_price = (
            Decimal('-1.'), Decimal('-1.')), Decimal('-1.')
        self.vol = Decimal('0.')

        # initialize binance connection
        self.client = Client(api_key=API_KEY, api_secret=API_SECRET)
        self.bm = BinanceSocketManager(self.client)

        # intialize queueing connection
        self.RABBIT_URI = os.getenv(
            "RABBIT_URI", "amqp://guest:guest@localhost/")

        print(self.client.get_asset_balance(self.quote),
              self.client.get_asset_balance(self.base), sep="\n")

    def stream_callback(self, msg: dict):
        """function processing the stream messages"""
        timestamp = datetime.fromtimestamp(int(msg['data']['E']) / 1000)
        if msg['stream'] == StreamType.LAST_PRICE:
            self.last_price = Decimal(
                msg['data']['a']), Decimal(msg['data']['b'])
            self.vol += min(Decimal(msg['data']['B']),
                            Decimal(msg['data']['A']))
        else:
            self.mark_price = Decimal(msg['data']['p'])
            ask, bid = self.last_price
            datapoint = {
                't': timestamp,
                'm': self.mark_price,
                'a': ask,
                'b': bid,
                'v': self.vol
            }

            # writing to queue
            asyncio.ensure_future(self.exchange.publish(Message(
                json.dumps(datapoint, cls=EnhancedJSONEncoder).encode(),
                delivery_mode=DeliveryMode.PERSISTENT
            ), routing_key=f'crypto.futures.tick.{self.currency.lower()}'), loop=self.loop)

            if timestamp.second:
                self.candles[-1].append(datapoint)
            else:
                self.cur_candle = self.candles[-1].copy()
                # create a candle
                asyncio.ensure_future(self.create_candle(), loop=self.loop)
                self.candles.append([datapoint])

            self.vol = Decimal('0.')  # reset volume for the next second

    async def create_candle(self):
        """Create an OHLC candle from a collection of ticks"""
        # NOTE: potentially calculate as `lambda tick: (tick['ask'] +
        # tick['bid'] / 2)` instead
        cur_candle = self.cur_candle
        candle = {
            't': cur_candle[0]['t'],
            'o': cur_candle[0]['m'],
            'h': max(map(lambda tick: tick['m'], cur_candle)),
            'l': min(map(lambda tick: tick['m'], cur_candle)),
            'c': cur_candle[-1]['m'],
            'v': sum(map(lambda tick: tick['v'], cur_candle)),
        }

        # FIXME: add provision for first candle (since length may not be == 1M)
        asyncio.ensure_future(self.exchange.publish(Message(
            json.dumps(candle, cls=EnhancedJSONEncoder).encode(),
            delivery_mode=DeliveryMode.PERSISTENT
        ), routing_key=f'crypto.futures.ohlc.{self.currency.lower()}'), loop=self.loop)
        self.cur_candle = list()

    async def run(self):
        """Initialize and start the streaming and calculation"""
        sockets = [
            self.bm.start_symbol_mark_price_socket(
                self.currency, self.stream_callback),  # mark price stream
            self.bm.start_symbol_ticker_futures_socket(
                self.currency, self.stream_callback),  # last price stream
        ]

        # setup rabbitmq exchange
        self.connection = await connect(self.RABBIT_URI, loop=self.loop)
        channel = await self.connection.channel()
        self.exchange = await channel.declare_exchange("tickers", ExchangeType.TOPIC)

        try:
            if all(sockets):
                self.bm.start()
                if not self.loop.is_running():
                    self.loop.run_forever()
            else:
                raise KeyboardInterrupt
        except KeyboardInterrupt:
            map(lambda socket: self.bm.stop_socket(
                socket) if socket else None, sockets)
            self.bm.close()
            self.loop.stop()
            await self.connection.close()
            reactor.stop()
