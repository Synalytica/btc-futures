#!/usr/bin/env python3
# coding: utf-8
"""
    :author: pk13055
    :brief: Basic test consumer to print out generated rabbitmq message(s)
"""
import argparse
import asyncio
import os
import sys

from aio_pika import connect, IncomingMessage, ExchangeType


loop = asyncio.get_event_loop()


def collect_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('-t','--topic', action='append', required=True,
                        help='topics to consume')
    args = parser.parse_args()
    return args


async def on_message(message: IncomingMessage):
    async with message.process():
        print("[x] %r" % message.body)


async def main(args: argparse.Namespace):
    # Perform connection
    RABBIT_URI = os.getenv("RABBIT_URI", "amqp://guest:guest@localhost/")
    connection = await connect(RABBIT_URI, loop=loop)

    # Creating a channel
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    exchange = await channel.declare_exchange(
        "tickers", ExchangeType.TOPIC
    )

    queues = []
    for topic in args.topic:
        queue = await channel.declare_queue(exclusive=True)
        await queue.bind(exchange, topic)
        queues.append(queue.consume(on_message))
    await asyncio.gather(*queues)


if __name__ == "__main__":
    args = collect_args()
    loop = asyncio.get_event_loop()
    loop.create_task(main(args))

    # we enter a never-ending loop that waits for data
    # and runs callbacks whenever necessary.
    print(" [*] Waiting for events. To exit press CTRL+C")
    loop.run_forever()
