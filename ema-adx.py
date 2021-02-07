#!/usr/bin/env python3
# coding: utf-8
"""
    :author: pk13055,mystertech
    :brief: basic strategy to outline connection to system
"""
import argparse
import asyncio
from collections import deque
from enum import Enum, IntEnum
import json
import os
import random

import numpy as np
import pandas as pd
from talib import stream

from utils.strategy_helpers import Strategy, create_streamer


class Signal(Enum):
    """Signal generated"""
    NULL = 0
    LONG = 1
    SHORT = 2


class CrossOver(IntEnum):
    """Types of EMA crossovers"""
    NULL = 0
    FOS = 1
    SOF = 2


args = None
_data = deque(maxlen=500)
trades = []
loop = asyncio.get_event_loop()
RABBIT_URI = os.getenv("RABBIT_URI", "amqp://guest:guest@localhost/")
on_stream = create_streamer(loop, RABBIT_URI)
strategy = Strategy("EMACross-ADX", loop)


def collect_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--ema_fast', type=int, default=10,
                        help="Fast moving ema period")
    parser.add_argument('--ema_slow', type=int, default=25,
                        help="Slow moving ema period")
    parser.add_argument('--adx', type=int, default=14,
                        help="ADX period")
    parser.add_argument('--sl', type=float, default=100,
                        help="ADX period")
    parser.add_argument('--tp', type=float, default=150,
                        help="ADX period")
    args = parser.parse_args()
    return args


def checkEntry(data: dict):
    """Checks entry after signal is generated"""
    if strategy.signal == Signal.LONG and data['m'] < data['b']:
        strategy.orderPrice = data['m']
        strategy.sl = strategy.orderPrice - args.sl
        strategy.tp = strategy.orderPrice + args.tp
        print(f"[entry][LONG] {data['t']} @ {strategy.orderPrice}\t|SL:\t{strategy.sl} :: TP:\t{strategy.tp}")
        strategy.signal = Signal.NULL
        return True
    elif strategy.signal == Signal.SHORT and data['m'] > data['a']:
        strategy.orderPrice = data['m']
        strategy.sl = strategy.orderPrice + args.sl
        strategy.tp = strategy.orderPrice - args.tp
        print(f"[entry][SHORT] {data['t']} @ {strategy.orderPrice}\t|SL:\t{strategy.sl} :: TP:\t{strategy.tp}")
        strategy.signal = Signal.NULL
        return True
    return False


def checkExit(data: dict):
    """Checks exit"""
    if strategy.sl < strategy.orderPrice and \
            (strategy.sl >= data['m'] or strategy.tp <= data['m']):
        if strategy.sl >= data['m']:
            print(f"[exit][LONG] {data['t']} @ {data['m']} [LOSS]")
        elif strategy.tp <= data['m']:
            print(f"[exit][LONG] {data['t']} @ {data['m']} [WIN]")
        return True
    elif strategy.sl > strategy.orderPrice and \
        (strategy.sl <= data['m'] or strategy.tp >= data['m']):
        if strategy.sl <= data['m']:
            print(f"[exit][SHORT] {data['t']} @ {data['m']} [LOSS]")
        elif strategy.tp >= data['m']:
            print(f"[exit][SHORT] {data['t']} @ {data['m']} [WIN]")
        return True
    return False


def genSig(data: dict):
    """Generates trade signals"""
    _data.append(data)
    global secLatestEMAFast, secLatestEMASlow
    df = pd.DataFrame.from_dict(_data)
    _data[-1].update({
        'emaF': (latestEMAFast := stream.EMA(df.c, timeperiod=strategy.eMAFast)),
        'emaS': (latestEMASlow := stream.EMA(df.c, timeperiod=strategy.eMASlow)),
        'adx': (latestADX := stream.ADX(df.h, df.l, df.c, timeperiod=strategy.aDXInterval)),
    })
    if latestEMAFast > latestEMASlow and \
            secLatestEMAFast < secLatestEMASlow and \
                latestADX < 40 and latestADX > 30:
        strategy.signal = Signal.SHORT
        strategy.sigGenerated = True
    elif latestEMAFast < latestEMASlow and \
            secLatestEMAFast > secLatestEMASlow and \
                latestADX < 40 and latestADX > 30:
        strategy.signal = Signal.LONG
        strategy.sigGenerated = True
    else :
        strategy.signal = Signal.NULL
        strategy.sigGenerated = False
    secLatestEMAFast, secLatestEMASlow = latestEMAFast, latestEMASlow
    

@on_stream(topic="crypto.futures.tick.btcusdt")
async def on_tick(data: dict):
    """Process data every tick"""
    if strategy.sigGenerated and not strategy.inPosition:
        if checkEntry(data):
            strategy.inPosition = True
            strategy.sigGenerated = False
    elif strategy.inPosition:
        if checkExit(data):
            strategy.inPosition = False


@on_stream(topic="crypto.futures.ohlc.btcusdt")
async def on_candle(data: dict):
    """Process data every candle"""
    genSig(data)


async def main():
    global args
    args = collect_args()

    strategy.overlap, strategy.sigGenerated, strategy.signal = CrossOver.NULL, False, Signal.NULL
    strategy.eMAFast, strategy.eMASlow, strategy.aDXInterval = args.ema_fast, args.ema_slow, args.adx
    strategy.orderPricem, strategy.sl, strategy.tp, secLatestEMAFast, secLatestEMASlow = 0, 0, 0, 0, 0

    loop.create_task(on_tick)
    loop.create_task(on_candle)



if __name__ == "__main__":
    loop.create_task(main())
    loop.run_forever()

