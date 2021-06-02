#!/usr/bin/env python3
# coding: utf-8
"""
    :author: shreygupta2809
    :brief: connect to database and API to provide past ohlc data

"""
import asyncio
from datetime import datetime
from functools import reduce
import json
from operator import add
import os
from typing import List, Dict


from aio_pika import connect, IncomingMessage, ExchangeType, Message, DeliveryMode
from aiohttp import ClientSession
import asyncio
import asyncpg
from throttler import throttle

from utils.encoder import EnhancedJSONDecoder, EnhancedJSONEncoder


class Database:
    """Listens and Returns Database and API ohlc data"""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
    ):
        self.DATABASE_URI = os.getenv(
            "DATABASE_URI", "postgresql://postgres:postgres@localhost/test"
        )
        self.RABBIT_URI = os.getenv("RABBIT_URI", "amqp://guest:guest@localhost/")
        self.loop = loop
        self.N_LIM = 1000
        self.BASE_URL = f"https://api.binance.com/api/v1/klines"

        self.topics = ["crypto.*.ohlc.*"]

    async def run(self) -> None:
        """Intialize and start the queue listeners"""
        self.pool = await asyncpg.create_pool(self.DATABASE_URI, timeout=60)

        self.connection = await connect(self.RABBIT_URI, loop=self.loop)
        channel = await self.connection.channel()
        await channel.set_qos(prefetch_count=1)

        self.exchange = await channel.declare_exchange("database", ExchangeType.TOPIC)

        tickers_channel = await self.connection.channel()
        self.exchange_tickers = await tickers_channel.declare_exchange(
            "tickers", ExchangeType.TOPIC
        )

        for topic in self.topics:
            queue = await channel.declare_queue()
            await queue.bind(self.exchange, topic)
            await queue.consume(self.on_message)

    async def on_message(self, message: IncomingMessage) -> None:
        """Process the message as it is delivered"""
        async with message.process():
            data = json.loads(message.body, cls=EnhancedJSONDecoder)
            message_params = message.routing_key.split(".")
            await self.query_ohlc_data(
                data,
                url=self.BASE_URL
                + f"?symbol={message_params[-1].upper()}&interval=1m&limit={self.N_LIM}",
                asset_type=message_params[1],
                currency=message_params[-1],
            )

    async def api_time_query(
        self, timestamps: list, url: str, asset_type: str, currency: str
    ) -> List[Dict]:
        """Get timestamp for query of the remaining data for backtest"""
        if not all(timestamps):
            return []

        _timestamps = []
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                async for row in conn.cursor(
                    f"""
                        (SELECT timestamp
                        FROM ohlc
                        ORDER BY timestamp DESC
                        LIMIT 1)

                        UNION ALL

                        (SELECT timestamp
                        FROM ohlc
                        ORDER BY timestamp ASC    
                        LIMIT 1);
                        """
                ):

                    _timestamps.append(row["timestamp"])

        _timestamps = list(map(lambda _time: _time.date(), _timestamps))
        _timestamps.sort()
        timestamps.sort()

        new_candles = []

        if timestamps[-1] < _timestamps[0] or timestamps[0] > _timestamps[-1]:
            new_candles = await self.gen_candles(
                timestamps[0], timestamps[-1], url, asset_type, currency
            )
        elif timestamps[0] < _timestamps[0] and timestamps[-1] <= _timestamps[-1]:
            new_candles = await self.gen_candles(
                timestamps[0], _timestamps[0], url, asset_type, currency
            )
        elif timestamps[0] >= _timestamps[0] and timestamps[-1] > _timestamps[-1]:
            new_candles = await self.gen_candles(
                _timestamps[-1], timestamps[-1], url, asset_type, currency
            )
        elif timestamps[0] < _timestamps[0] and timestamps[-1] > _timestamps[-1]:
            new_candles = await self.gen_candles(
                timestamps[0], _timestamps[0], url, asset_type, currency
            )
            new_candles.extend(
                await self.gen_candles(
                    _timestamps[-1], timestamps[-1], url, asset_type, currency
                )
            )

        return new_candles

    @throttle(rate_limit=1200, period=60.0)
    async def fetch_url(self, url: str, session: ClientSession) -> dict:
        """Get response from url"""
        async with session.get(url) as response:
            return await response.json()

    async def gen_candles(
        self,
        start_time: datetime.date,
        end_time: datetime.date,
        url: str,
        asset_type: str,
        currency: str,
    ) -> List[Dict]:
        """Generate list of ohlc candles"""
        start_time: int = int(
            datetime(start_time.year, start_time.month, start_time.day).timestamp()
        )
        end_time: int = int(
            datetime(end_time.year, end_time.month, end_time.day).timestamp()
        )

        req_params = [
            (start * 1000, (start + self.N_LIM * 60) * 1000)
            for start in range(start_time, end_time, self.N_LIM * 60)
        ]
        urls = map(
            lambda lims: url + f"&startTime={lims[0]}&endTime={lims[-1]}",
            req_params,
        )
        fields = ["t", "o", "h", "l", "c", "v"]

        async with ClientSession() as session:
            chunks = map(
                lambda url: asyncio.ensure_future(self.fetch_url(url, session)),
                urls,
            )
            candles = reduce(add, await asyncio.gather(*chunks))
            _candles = list(
                map(
                    lambda candle: dict(
                        zip(
                            fields,
                            [datetime.fromtimestamp(int(candle[0] / 1000))]
                            + list(map(float, candle[1 : len(fields)])),
                        )
                    ),
                    candles,
                )
            )

            # Send new data to warehoose for storage in DB
            asyncio.ensure_future(
                self.exchange_tickers.publish(
                    Message(
                        json.dumps(_candles, cls=EnhancedJSONEncoder).encode(),
                        delivery_mode=DeliveryMode.PERSISTENT,
                    ),
                    routing_key=f"crypto.{asset_type}.ohlc.{currency}",
                ),
                loop=self.loop,
            )

            return _candles

    async def query_ohlc_data(
        self, data: list, url: str, asset_type: str, currency: str
    ) -> None:
        """Get the past ohlc data from the DB and API"""
        _candles = []
        if all(data):
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    async for row in conn.cursor(
                        f"""
                                                    SELECT * from ohlc
                                                    WHERE timestamp >= '{data[0]}' AND timestamp < '{data[-1]}'
                                                    ORDER BY timestamp;
                                                    """
                    ):

                        datapoint = {
                            "t": row["timestamp"],
                            "o": row["open"],
                            "h": row["high"],
                            "l": row["low"],
                            "c": row["close"],
                            "v": row["vol"],
                        }
                        _candles.append(datapoint)

        new_old_candles = await self.api_time_query(data, url, asset_type, currency)
        new_old_candles.extend(_candles)

        # Send new + old data back to calling/listening function
        asyncio.ensure_future(
            self.exchange.publish(
                Message(
                    json.dumps(new_old_candles, cls=EnhancedJSONEncoder).encode(),
                    delivery_mode=DeliveryMode.PERSISTENT,
                ),
                routing_key=f"crypto.{asset_type}.ohlc.{currency}.db",
            ),
            loop=self.loop,
        )
