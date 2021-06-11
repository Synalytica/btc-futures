"""
    :author: pk13055, shreygupta2809
    :brief: convenience functions for strategy help
"""
from datetime import datetime, timedelta
import hashlib
import json
import os
import uuid

from aio_pika import connect, IncomingMessage, ExchangeType, Message, DeliveryMode
import asyncio

from .encoder import EnhancedJSONDecoder, EnhancedJSONEncoder
from .enums import Stage, StrategyType


class Strategy:
    """Base class for strategy creation"""

    def __init__(
        self,
        stage: Stage,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
        params: dict = None,
    ):
        self.name = params["name"]
        self.strategy_type = StrategyType(params["strategy_type"].lower())
        self.loop = loop
        self.stage = Stage[stage]
        self.asset = params["asset"]
        self.asset_class = params["asset_class"]
        self.asset_type = params["asset_type"]
        self.inPosition: bool = False
        self.DATABASE_URI: str = os.getenv(
            "DATABASE_URI", "postgresql://postgres@localhost/test"
        )
        self.RABBIT_URI: str = os.getenv("RABBIT_URI", "amqp://guest:guest@localhost/")
        self.sigGenerated: bool = False

        self.versionID = hashlib.md5(
            json.dumps(params, sort_keys=True).encode("utf-8")
        ).hexdigest()
        # TODO: Generate Strategy ID using proper methods from config
        self.strategyID = uuid.uuid4().hex

        self.start_date = params["start_date"]
        self.end_date = params["end_date"]
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
            publish_data = {
                "date_interval": [self.start_date, self.end_date],
                "asset": self.asset,
                "frequency": "1m",
                "asset_type": self.asset_type,
                "versionID": self.versionID,
            }
            asyncio.ensure_future(
                self.exchange.publish(
                    Message(
                        json.dumps(publish_data, cls=EnhancedJSONEncoder).encode(),
                        delivery_mode=DeliveryMode.PERSISTENT,
                    ),
                    routing_key=f"{self.asset_class}.meta.{self.strategyID}.requests.{self.versionID}",
                ),
                loop=self.loop,
            )

    async def create_exchanges(self) -> None:
        """Creates the Exchanges, Pool and Topics necessary for execution"""
        self.connection = await connect(self.RABBIT_URI, loop=self.loop)
        channel = await self.connection.channel()
        if self.stage == Stage.LIVE:
            self.ohlc_topic = (
                f"{self.asset_class}.tickers.{self.asset_type}.ohlc.1m.{self.asset}"
            )
            self.tick_topic = (
                f"{self.asset_class}.tickers.{self.asset_type}.tick.{self.asset}"
            )
            self.exchange_name = "tickers"
        elif self.stage == Stage.BACKTEST:
            self.ohlc_topic = f"{self.asset_class}.tickers.{self.asset_type}.ohlc.1m.{self.asset}.{self.versionID}"
            self.tick_topic = f"{self.asset_class}.tickers.{self.asset_type}.tick.{self.asset}.{self.versionID}"
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
            if ".tick." in message.routing_key:
                await self.on_tick(data)
            elif ".ohlc." in message.routing_key:
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
            data = list({frozenset(item.items()): item for item in data}.values())
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
