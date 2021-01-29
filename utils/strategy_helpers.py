"""
    :author: pk13055
    :brief: convenience functions for strategy help
"""
import asyncio
from copy import copy
from dataclasses import dataclass
import json

from aio_pika import connect, IncomingMessage, ExchangeType

from .encoder import EnhancedJSONDecoder


@dataclass
class Strategy:
    """Base class for strategy creation"""
    name: str
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    inPosition: bool = False


def create_streamer(loop: asyncio.AbstractEventLoop, RABBIT_URI: str):
    """Create a streaming object decorator for tick and candle processing"""
    def on_stream(topic: str):
        async def decorator(function):
            connection = await connect(RABBIT_URI, loop=loop)
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)
            exchange = await channel.declare_exchange(
                "tickers", ExchangeType.TOPIC, passive=True
            )
            queue = await channel.declare_queue()
            await queue.bind(exchange, topic)

            async def wrapper(message: IncomingMessage, *args, **kwargs):
                async with message.process():
                    data = json.loads(
                        message.body, cls=EnhancedJSONDecoder)
                    await function(data, **kwargs)
            await queue.consume(wrapper)
        return decorator
    return on_stream
