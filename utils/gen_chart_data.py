#!/usr/bin/env python3
# coding: utf-8
"""
:author: pk13055
:brief: generate charting json data from input

"""
import argparse
import asyncio
from datetime import datetime
from functools import reduce
from sys import argv as rd
import json
from operator import add
from analysis import generate_metrics

from aiohttp import ClientSession
import numpy as np
import pandas as pd

N_LIM = 1500  # NOTE: candle fetch limit per call
BASE_URL = f"https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit={N_LIM}"

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.Timedelta):
            return obj.isoformat()
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)

def collect_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, default="orders.csv", help="Order data")
    parser.add_argument("-c", "--candles", type=str, default="candles.csv", help="Candle data dump")
    parser.add_argument("-o", "--output", type=str, default="data.json", help="Output charting json")
    args = parser.parse_args()
    return args


async def fetch_url(url: str, session: ClientSession) -> dict:
    async with session.get(url) as response:
        return await response.json()


async def gen_candles(start_time: int, end_time: int) -> list:
    """Generate list of ohlc candles"""
    req_params = [(start * 1000, (start + N_LIM * 60) * 1000) for start in range(start_time, end_time, N_LIM * 60)]
    urls = map(lambda lims: BASE_URL + f"&startTime={lims[0]}&endTime={lims[-1]}", req_params)


async def fetch_url(url: str, session: ClientSession) -> dict:
    async with session.get(url) as response:
        return await response.json()


async def gen_candles(start_time: int, end_time: int) -> list:
    """Generate list of ohlc candles"""
    req_params = [(start * 1000, (start + N_LIM * 60) * 1000) for start in range(start_time, end_time, N_LIM * 60)]
    urls = map(lambda lims: BASE_URL + f"&startTime={lims[0]}&endTime={lims[-1]}", req_params)
    fields = ["time", "open", "high", "low", "close", "volume"]
    async with ClientSession() as session:
        chunks = map(lambda url: asyncio.ensure_future(fetch_url(url, session)), urls)
        candles = reduce(add, await asyncio.gather(*chunks))
        return map(lambda candle: dict(zip(fields, [int(candle[0] / 1000)] + candle[1 : len(fields)])), candles)


def parse_orders(filename) -> dict:
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
    return orders, metrics, df.entry_time.min(), df.exit_time.max()


async def main():
    args = collect_args()

    orders, metrics, start_time, end_time = parse_orders(args.input)
    candles = await gen_candles(start_time, end_time)

    ohlc = pd.DataFrame.from_dict(list(candles)).set_index("time").sort_index()
    ohlc.to_csv(args.candles)

    json.dump({
        "candles": ohlc.reset_index().to_dict(orient="records"),
        "orders": orders,
        "metrics": metrics.reset_index().to_dict(orient="records"),
    }, open(args.output, "w+"), cls=NpEncoder)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
