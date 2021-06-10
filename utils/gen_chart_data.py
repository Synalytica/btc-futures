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
import uuid


from aio_pika import connect, IncomingMessage, ExchangeType, Message, DeliveryMode
import asyncio
import pandas as pd

from analysis import generate_metrics
from encoder import NpEncoder, EnhancedJSONEncoder, EnhancedJSONDecoder
from enums import MessageCounter


loop = asyncio.get_event_loop()
args = None
exchange = None
old_candles: List[Dict] = []
orders: List[Dict] = []
metrics = None
messages: int = MessageCounter.RESET


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
    parser.add_argument(
        "-a", "--asset", type=str, default="btcusdt", help="Asset to Get data for"
    )
    parser.add_argument(
        "-f", "--frequency", type=str, default="1m", help="Candle frequency $num[m/h/d]"
    )
    parser.add_argument(
        "--asset_class",
        type=str,
        default="crypto",
        help="Asset Class [crypto/stock/forex/commodities]",
    )
    parser.add_argument(
        "-t",
        "--type",
        type=str,
        default="futures",
        help="Asset type [futures/spot/margin]",
    )
    args = parser.parse_args()
    return args


async def candle_handler(new_candles: List[Dict]) -> None:
    """Updates the candles and on completion writes to file"""
    global old_candles, messages
    old_candles.extend(new_candles)
    messages += MessageCounter.RECEIVE
    if messages <= 0:
        ohlc = (
            pd.DataFrame.from_dict(list(old_candles))
            .set_index("Timestamp")
            .sort_index()
        )
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
        raise sys.exit()


async def on_message(message: IncomingMessage, *args, **kwargs) -> None:
    """Callback function which routes message to necessary function"""
    async with message.process():
        data = json.loads(message.body, cls=EnhancedJSONDecoder)
        df = pd.DataFrame(data)
        df.drop_duplicates()
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


async def setup_exchanges() -> dict:
    """Setups the necessary exchanges and binds the queues"""
    RABBIT_URI = os.getenv("RABBIT_URI", "amqp://guest:guest@localhost/")
    connection = await connect(RABBIT_URI, loop=loop)
    channel = await connection.channel()

    # TODO: Better ID Creation
    versionID = uuid.uuid4().hex
    strategyID = uuid.uuid4().hex

    topic = f"{args.asset_class}.tickers.{args.type}.ohlc.{args.frequency}.{args.asset}.{versionID}"

    global exchange
    exchange = await channel.declare_exchange("database", ExchangeType.TOPIC)

    await channel.set_qos(prefetch_count=1)

    queue = await channel.declare_queue()
    await queue.bind(exchange, topic)
    await queue.consume(on_message)

    return (strategyID, versionID)


def publish_candle_message(
    strategyID: str,
    versionID: str,
    start_time: datetime.date = None,
    end_time: datetime.date = None,
) -> None:
    """Publish messages to fetch data from DB and API"""
    publish_data = {
        "date_interval": [start_time, end_time],
        "asset": args.asset,
        "frequency": args.frequency,
        "asset_type": args.type,
        "versionID": versionID,
    }
    asyncio.ensure_future(
        exchange.publish(
            Message(
                json.dumps(publish_data, cls=EnhancedJSONEncoder).encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
            ),
            routing_key=f"{args.asset_class}.meta.{strategyID}.requests.{versionID}",
        ),
        loop=loop,
    )


def gen_candles(
    start_time: datetime.date,
    end_time: datetime.date,
    candle_start_time: datetime.date,
    candle_end_time: datetime.date,
    strategyID: str,
    versionID: str,
) -> None:
    """Generates candle start and end times and number of messages"""
    global messages
    if candle_end_time == None and candle_start_time == None:
        publish_candle_message(strategyID, versionID, start_time, end_time)
        messages += MessageCounter.ADD
    elif end_time <= candle_end_time and start_time >= candle_start_time:
        publish_candle_message(strategyID, versionID)
    elif end_time < candle_start_time or start_time > candle_end_time:
        publish_candle_message(strategyID, versionID, start_time, end_time)
        messages += MessageCounter.ADD
    elif start_time < candle_start_time and end_time <= candle_end_time:
        publish_candle_message(strategyID, versionID, start_time, candle_start_time)
        messages += MessageCounter.ADD
    elif start_time >= candle_start_time and end_time > candle_end_time:
        publish_candle_message(strategyID, versionID, candle_end_time, end_time)
        messages += MessageCounter.ADD
    else:
        publish_candle_message(strategyID, versionID, start_time, candle_start_time)
        messages += MessageCounter.ADD
        publish_candle_message(strategyID, versionID, candle_end_time, end_time)
        messages += MessageCounter.ADD


async def parse_orders(filename: str) -> dict:
    """Parse Orders.csv to get metrics and orders"""
    df = pd.read_csv(filename, parse_dates=[0, 1])
    metrics = generate_metrics(df)
    df.entry_time = df.entry_time.round("T").map(lambda x: int(datetime.timestamp(x)))
    df.exit_time = df.exit_time.round("T").map(lambda x: int(datetime.timestamp(x)))
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

    global metrics, orders
    orders, metrics, start_time, end_time = await parse_orders(args.input)

    global old_candles
    candle_end_time: datetime.date = None
    candle_start_time: datetime.date = None

    if args.candles:
        candle_start_time, candle_end_time, old_candles = await parse_candles(
            args.candles
        )

    strategyID, versionID = await setup_exchanges()

    gen_candles(
        start_time, end_time, candle_start_time, candle_end_time, strategyID, versionID
    )


if __name__ == "__main__":
    loop.create_task(main())
    loop.run_forever()
