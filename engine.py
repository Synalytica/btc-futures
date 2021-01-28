#!/usr/bin/env python3
# coding: utf-8
"""
    :author: mystertech, pk13055
    :brief: connect to binance live streaming data through websockets

"""
import asyncio
from decimal import Decimal
from datetime import datetime
import os
import sys

from aio_pika import connect, Message, DeliveryMode, ExchangeType
from binance.client import Client
from binance.websockets import BinanceSocketManager
from twisted.internet import reactor

from stream import CandleMaker, CandleDumper, Stream, ohlc, ticks, candles


class Engine:
    """Engine to run the streaming and strategy functionality"""

    def __init__(self, API_KEY: str, API_SECRET: str):
        self.last_price, self.mark_price = (
            Decimal('-1.'), Decimal('-1.')), Decimal('-1.')
        self.vol = Decimal('0.')
        self.candle_stream = CandleMaker()
        self.candle_dump = CandleDumper()

        # initialize binance connection
        self.client = Client(api_key=API_KEY, api_secret=API_SECRET)
        self.bm = BinanceSocketManager(self.client)

        # intialize queueing connection
        self.RABBIT_URI = os.getenv(
            "RABBIT_URI", "amqp://guest:guest@localhost/")

        print(self.client.get_asset_balance("BTC"),
              self.client.get_asset_balance("USDT"), sep="\n")

    def stream_callback(self, msg: dict):
        """function processing the stream messages"""
        timestamp = datetime.fromtimestamp(int(msg['data']['E']) / 1000)
        if msg['stream'] == Stream.LAST_PRICE:
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
            # TODO: put tick into relevant queue
            await self.exchange.publish(Message(

            ), routing_key='crypto.futures.tick.btcusdt')
            ticks.put_nowait(datapoint)
            if timestamp.tm_sec:
                candles[-1].append(datapoint)
            else:
                candles.append([datapoint])
            self.vol = Decimal('0.')  # reset volume for the next second

    async def run(self):
        """Initialize and start the streaming and calculation"""
        sockets = [
            self.bm.start_symbol_mark_price_socket(
                'BTCUSDT', self.stream_callback),  # mark price stream
            self.bm.start_symbol_ticker_futures_socket(
                'BTCUSDT', self.stream_callback),  # last price stream
        ]

        # setup rabbitmq exchange
        self.connection = await connect(self.RABBIT_URI, loop=asyncio.get_event_loop())
        channel = await self.connection.channel()
        self.exchange = await channel.declare_exchange("tickers", ExchangeType.TOPIC, passive=True)

        try:
            if all(sockets):
                self.candle_stream.start()
                self.bm.start()
                await self.candle_dump.run()
            else:
                raise KeyboardInterrupt
        except KeyboardInterrupt:
            map(lambda socket: self.bm.stop_socket(
                socket) if socket else None, sockets)
            self.bm.close()
            self.candle_stream.join()
            self.candle_dump.join()
            await self.connection.close()
            reactor.stop()
