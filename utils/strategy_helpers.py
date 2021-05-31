"""
    :author: pk13055,shreygupta2809
    :brief: convenience functions for strategy help
"""
from datetime import datetime, timedelta
import json
import os

from aio_pika import connect, IncomingMessage, ExchangeType, Message, DeliveryMode
import asyncio

from .encoder import EnhancedJSONDecoder, EnhancedJSONEncoder
from .enums import Stage


class Strategy:
    """Base class for strategy creation"""

    def __init__(
        self,
        name: str,
        stage: Stage,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
        currency: str = "btcusdt",
        asset: str = "crypto",
        asset_type: str = "futures",
        start_date: datetime = "",
        end_date: datetime = "",
    ):
        self.name = name
        self.loop = loop
        self.stage = Stage[stage]
        self.currency = currency
        self.asset = asset
        self.asset_type = asset_type
        self.inPosition: bool = False
        self.DATABASE_URI: str = os.getenv(
            "DATABASE_URI", "postgresql://postgres@localhost/test"
        )
        self.RABBIT_URI: str = os.getenv("RABBIT_URI", "amqp://guest:guest@localhost/")
        self.sigGenerated: bool = False

        self.start_date = start_date
        self.end_date = end_date
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
                self.end_date = datetime.strptime(self.end_date, "%Y/%m/%d")

            self.start_date = self.start_date.date()
            self.end_date = self.end_date.date()

    async def run(self) -> None:
        """The main strategy run function which executes the strategy"""
        await self.create_exchanges()
        await self.bind_queues()

        if self.stage == Stage.BACKTEST:
            asyncio.ensure_future(
                self.exchange.publish(
                    Message(
                        json.dumps(
                            [self.start_date, self.end_date], cls=EnhancedJSONEncoder
                        ).encode(),
                        delivery_mode=DeliveryMode.PERSISTENT,
                    ),
                    routing_key=f"{self.asset}.{self.asset_type}.ohlc.{self.currency}",
                ),
                loop=self.loop,
            )

    async def create_exchanges(self) -> None:
        """Creates the Exchanges, Pool and Topics necessary for execution"""
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

    async def bind_queues(self) -> None:
        """Binds the Queues to the Exchange for Necessary Topics"""
        channel = await self.connection.channel()
        await channel.set_qos(prefetch_count=1)

        self.topics = [self.ohlc_topic]
        if self.stage != Stage.BACKTEST:
            self.topics.append(self.tick_topic)

        for topic in self.topics:
            queue = await channel.declare_queue()
            await queue.bind(self.exchange, topic)
            await queue.consume(self.on_message)

    async def on_message(self, message: IncomingMessage, *args, **kwargs) -> None:
        """Callback function which routes message to necessary function"""
        async with message.process():
            data = json.loads(message.body, cls=EnhancedJSONDecoder)
            if "tick" in message.routing_key:
                await self.on_tick(data)
            elif "ohlc" in message.routing_key:
                await self.on_candle(data)

    async def on_tick(self, data: dict) -> None:
        """Process data every tick"""
        if self.sigGenerated and not self.inPosition:
            if self.checkEntry(data):
                self.inPosition = True
                self.sigGenerated = False
        elif self.inPosition:
            if self.checkExit(data):
                self.inPosition = False

    async def on_candle(self, data: dict) -> None:
        """Process data every candle"""
        if self.stage == Stage.LIVE:
            self.genSig(data)
        elif self.stage == Stage.BACKTEST:
            sorted_data = sorted(data, key=lambda k: k["t"])
            self.backtest(sorted_data)

    def checkEntry(self, data: dict) -> None:
        """Checks entry after signal is generated"""
        raise NotImplementedError

    def checkExit(self, data: dict) -> None:
        """Checks exit"""
        raise NotImplementedError

    def genSig(self, data: dict) -> None:
        """Generates trade signals"""
        raise NotImplementedError

    def backtest(self, data: dict) -> None:
        """Backtests the strategy on past ohlc data"""
        raise NotImplementedError
