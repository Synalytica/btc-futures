"""
    :author: pk13055
    :brief: convenience functions for strategy help
"""
import asyncio
from copy import copy
from dataclasses import dataclass
import json
import os
from datetime import datetime, timedelta
from enum import Enum, IntEnum

from aio_pika import connect, IncomingMessage, ExchangeType, Message, DeliveryMode

import asyncio
import asyncpg

from .encoder import EnhancedJSONDecoder, EnhancedJSONEncoder


class Stage(Enum):
    """Stage of the Strategy"""

    BACKTEST = 0
    OPTIMIZE = 1
    PAPER = 2
    LIVE = 3
    ARCHIVE = 4
    LIQUIDATE = 5  # CLOSE


def stage_to_enum(argument):
    switcher = {
        "B": Stage.BACKTEST,
        "O": Stage.OPTIMIZE,
        "P": Stage.PAPER,
        "L": Stage.LIVE,
        "A": Stage.ARCHIVE,
        "C": Stage.LIQUIDATE,
    }

    return switcher.get(argument, Stage.BACKTEST)


@dataclass
class Strategy:
    """Base class for strategy creation"""

    name: str
    stage: str
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    inPosition: bool = False
    DATABASE_URI: str = os.getenv(
        "DATABASE_URI", "postgresql://postgres@localhost/test"
    )
    RABBIT_URI: str = os.getenv("RABBIT_URI", "amqp://guest:guest@localhost/")
    currency: str = "btcusdt"
    asset: str = "crypto"
    asset_type: str = "future"
    sigGenerated: bool = False
    start_date: datetime = ""
    end_date: datetime = ""

    def __post_init__(self):
        self.stage = stage_to_enum(self.stage)
        if self.stage == Stage.BACKTEST:
            if self.start_date == "" and self.end_date != "":
                self.end_date = datetime.strptime(self.end_date, "%Y/%m/%d")
                self.start_date = datetime.strptime(
                    self.end_date, "%Y/%m/%d"
                ) - timedelta(2)

            elif self.start_date != "" and self.end_date == "":
                self.start_date = datetime.strptime(self.start_date, "%Y/%m/%d")
                self.end_date = self.start_date + timedelta(2)

            elif self.start_date == "" and self.end_date == "":
                self.end_date = datetime.today()
                self.start_date = datetime.today() - timedelta(2)

            else:
                self.start_date = datetime.strptime(self.start_date, "%Y/%m/%d")
                self.end_date = datetime.strptime(
                    self.end_date, "%Y/%m/%d"
                ) + timedelta(1)

            self.start_date = self.start_date.date()
            self.end_date = self.end_date.date()

    async def run(self):
        await self.create_exchanges()
        on_stream = create_streamer(self.loop, self.RABBIT_URI, self.exchange_name)

        @on_stream(topic=self.tick_topic)
        async def on_tick(data: dict):
            """Process data every tick"""
            print("TICK: ", data)
            if self.sigGenerated and not self.inPosition:
                if self.checkEntry(data):
                    self.inPosition = True
                    self.sigGenerated = False
            elif self.inPosition:
                if self.checkExit(data):
                    self.inPosition = False

        @on_stream(topic=self.ohlc_topic)
        async def on_candle(data: dict):
            """Process data every candle"""
            print("CANDLE: ", data)
            if self.stage == Stage.LIVE:
                self.genSig(data)
            elif self.stage == Stage.BACKTEST:
                self.backtest(data)

        if self.stage == Stage.BACKTEST:
            self.loop.create_task(self.query_ohlc_data())
        elif self.stage == Stage.LIVE:
            self.loop.create_task(on_tick)
        self.loop.create_task(on_candle)

    async def create_exchanges(self):
        self.connection = await connect(self.RABBIT_URI, loop=self.loop)
        channel = await self.connection.channel()
        if self.stage == Stage.LIVE:
            self.ohlc_topic = f"{self.asset}.{self.asset_type}.ohlc.{self.currency}"
            self.tick_topic = f"{self.asset}.{self.asset_type}.tick.{self.currency}"
            self.exchange_name = "tickers"
        elif self.stage == Stage.BACKTEST:
            self.ohlc_topic = f"{self.asset}.{self.asset_type}.ohlc.{self.currency}.db"
            self.tick_topic = f"{self.asset}.{self.asset_type}.tick.{self.currency}.db"
            self.exchange_name = "database"

        self.exchange = await channel.declare_exchange(
            self.exchange_name, ExchangeType.TOPIC
        )

    def checkEntry(self, data: dict):
        """Checks entry after signal is generated"""
        pass

    def checkExit(self, data: dict):
        """Checks exit"""
        pass

    def genSig(self, data: dict):
        """Generates trade signals"""
        pass

    def backtest(self, data: dict):
        """Backtests the strategy on past ohlc data"""
        pass

    async def query_ohlc_data(self):
        async with asyncpg.create_pool(self.DATABASE_URI, command_timeout=60) as pool:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    async for row in conn.cursor(
                        f"""
                                                SELECT * from ohlc
                                                WHERE timestamp >= '{self.start_date}' AND timestamp < '{self.end_date}'
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

                        asyncio.ensure_future(
                            self.exchange.publish(
                                Message(
                                    json.dumps(
                                        datapoint, cls=EnhancedJSONEncoder
                                    ).encode(),
                                    delivery_mode=DeliveryMode.PERSISTENT,
                                ),
                                routing_key=f"{self.asset}.{self.asset_type}.ohlc.{self.currency}.db",
                            ),
                            loop=self.loop,
                        )


def create_streamer(
    loop: asyncio.AbstractEventLoop, RABBIT_URI: str, exchange_name: str
):
    """Create a streaming object decorator for tick and candle processing"""

    def on_stream(topic: str):
        async def decorator(function):
            connection = await connect(RABBIT_URI, loop=loop)
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)
            exchange = await channel.declare_exchange(exchange_name, ExchangeType.TOPIC)
            queue = await channel.declare_queue()
            await queue.bind(exchange, topic)

            async def wrapper(message: IncomingMessage, *args, **kwargs):
                async with message.process():
                    data = json.loads(message.body, cls=EnhancedJSONDecoder)
                    await function(data, **kwargs)

            await queue.consume(wrapper)

        return decorator

    return on_stream
