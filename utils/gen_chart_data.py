#!/usr/bin/env python3
# coding: utf-8
"""
:author: pk13055, shreygupta2809
:brief: generate charting json data from input
"""
import argparse
import asyncio
from datetime import datetime
import sys
import json
import os
from typing import List, Dict


from aio_pika import connect, IncomingMessage, ExchangeType, Message, DeliveryMode
import asyncio
import pandas as pd

from analysis import generate_metrics
from encoder import NpEncoder, EnhancedJSONEncoder, EnhancedJSONDecoder


loop = asyncio.get_event_loop()
args = None
exchange = None
old_candles: List[Dict] = []
orders: List[Dict] = []
metrics = None
messages: int = 0


def collect_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i", "--input", type=str, default="orders.csv", help="Order data"
    )
    parser.add_argument(
        "-c", "--candles", type=str, default="", help="Candle data dump"
    )
    parser.add_argument(
        "-o", "--output", type=str, default="data.json", help="Output charting json"
    )
    args = parser.parse_args()
    return args


def file_write():
    """Create and the write to the output files"""
    global old_candles, metrics, orders
    ohlc = pd.DataFrame.from_dict(list(old_candles)).set_index("Timestamp").sort_index()
    candles_out_file = args.candles if args.candles != "" else "candles.csv"
    ohlc.to_csv(candles_out_file)

    json.dump(
        {
            "candles": ohlc.reset_index().to_dict(orient="records"),
            "orders": orders,
            "metrics": metrics.reset_index().to_dict(orient="records"),
        },
        open(args.output, "w+"),
        cls=NpEncoder,
    )


async def candle_handler(new_candles: List[Dict]) -> None:
    """Updates the candles and on completion writes to file"""
    global old_candles, messages
    old_candles.extend(new_candles)
    messages -= 1
    if messages <= 0:
        file_write()
        raise sys.exit()


async def on_message(message: IncomingMessage, *args, **kwargs) -> None:
    """Callback function which routes message to necessary function"""
    async with message.process():
        data = json.loads(message.body, cls=EnhancedJSONDecoder)
        df = pd.DataFrame(data)
        df = df.rename(
            index=str,
            columns={
                "t": "Timestamp",
                "o": "Open",
                "h": "High",
                "l": "Low",
                "c": "Close",
                "v": "Volume",
            },
        )
        await candle_handler(df.to_dict(orient="records"))


async def setup_exchanges() -> None:
    """Setups the necessary exchanges and binds the queues"""
    RABBIT_URI = os.getenv("RABBIT_URI", "amqp://guest:guest@localhost/")
    connection = await connect(RABBIT_URI, loop=loop)
    channel = await connection.channel()
    topic = "crypto.futures.ohlc.btcusdt.db"

    global exchange
    exchange = await channel.declare_exchange("database", ExchangeType.TOPIC)

    await channel.set_qos(prefetch_count=1)

    queue = await channel.declare_queue()
    await queue.bind(exchange, topic)
    await queue.consume(on_message)


def publish_candle_message(
    start_time: datetime.date = None, end_time: datetime.date = None
) -> None:
    """Publish messages to fetch data from DB and API"""
    asyncio.ensure_future(
        exchange.publish(
            Message(
                json.dumps([start_time, end_time], cls=EnhancedJSONEncoder).encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
            ),
            routing_key=f"crypto.futures.ohlc.btcusdt",
        ),
        loop=loop,
    )


def gen_candles(
    start_time: datetime.date,
    end_time: datetime.date,
    candle_start_time: datetime.date,
    candle_end_time: datetime.date,
) -> None:
    """Generates candle start and end times and number of messages"""
    global messages
    if candle_end_time == None and candle_start_time == None:
        publish_candle_message(start_time, end_time)
        messages = 1
    elif end_time <= candle_end_time and start_time >= candle_start_time:
        publish_candle_message()
        messages = 0
    elif end_time < candle_start_time or start_time > candle_end_time:
        publish_candle_message(start_time, end_time)
        messages = 1
    elif start_time < candle_start_time and end_time <= candle_end_time:
        publish_candle_message(start_time, candle_start_time)
        messages = 1
    elif start_time >= candle_start_time and end_time > candle_end_time:
        publish_candle_message(candle_end_time, end_time)
        messages = 1
    else:
        publish_candle_message(start_time, candle_start_time)
        publish_candle_message(candle_end_time, end_time)
        messages = 2


async def parse_orders(filename: str) -> dict:
    """Parse Orders.csv to get metrics and orders"""
    df = pd.read_csv(filename, parse_dates=[0, 1])
    metrics = generate_metrics(df)
    df.entry_time = df.entry_time.round("T").map(lambda x: int(x.timestamp()))
    df.exit_time = df.exit_time.round("T").map(lambda x: int(x.timestamp()))
    orders = []
    for _, trade in df.iterrows():
        entry = {
            "time": int(trade.entry_time),
            "position": "belowBar" if trade.trade_type == "LONG" else "aboveBar",
            "color": "green" if trade.status else "red",
            "shape": "arrowUp" if trade.trade_type == "LONG" else "arrowDown",
            "id": f"id{_}-entry",
            "text": f"Entry @ {trade.entry_price: 0.3f}",
            "size": 0.8,
        }
        orders.append(entry)
        exit = entry.copy()
        exit.update(
            {
                "time": int(trade.exit_time),
                "position": "aboveBar" if trade.trade_type == "LONG" else "belowBar",
                "shape": "arrowUp" if trade.trade_type == "SHORT" else "arrowDown",
                "id": f"id{_}-exit",
                "text": f"Exit @ {trade.exit_price: 0.3f}",
                "size": 0.5,
            }
        )
        orders.append(exit)
    return (
        orders,
        metrics,
        datetime.fromtimestamp(df.entry_time.min()).date(),
        datetime.fromtimestamp(df.exit_time.max()).date(),
    )


async def parse_candles(filename: str) -> dict:
    """Get candles first and last time with old candles"""
    df = pd.read_csv(filename, parse_dates=[0])
    return (
        df.Timestamp.min().date(),
        df.Timestamp.max().date(),
        df.to_dict(orient="records"),
    )


async def main():
    global args
    args = collect_args()
    await setup_exchanges()

    global orders, metrics
    orders, metrics, start_time, end_time = await parse_orders(args.input)

    global old_candles
    candle_end_time: datetime.date = None
    candle_start_time: datetime.date = None

    if args.candles:
        candle_start_time, candle_end_time, old_candles = await parse_candles(
            args.candles
        )

    gen_candles(start_time, end_time, candle_start_time, candle_end_time)


if __name__ == "__main__":
    loop.create_task(main())
    loop.run_forever()
