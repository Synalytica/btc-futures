#!/usr/bin/env python3
# coding: utf-8
"""
    :author: pk13055
    :brief: Basic test producer to generate rabbitmq message(s)
"""
import asyncio
import os
import sys

from aio_pika import connect, Message, DeliveryMode, ExchangeType


async def main(loop):
    # Perform connection
    RABBIT_URI = os.getenv("RABBIT_URI", "amqp://guest:guest@localhost/")
    connection = await connect(RABBIT_URI, loop=loop)

    # Creating a channel
    channel = await connection.channel()

    exchange = await channel.declare_exchange(
        "tickers", ExchangeType.TOPIC, passive=True
    )

    message_body = (" ".join(sys.argv[2:]) or "Hello World!").encode()

    message = Message(
        message_body,
        delivery_mode=DeliveryMode.PERSISTENT
    )

    # Sending the message
    await exchange.publish(message, routing_key=sys.argv[1])

    print(" [x] Sent %r" % message)

    await connection.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
