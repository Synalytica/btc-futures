#!/usr/bin/env python3
# coding: utf-8
"""
    :author: pk13055,mystertech
    :brief: basic strategy to outline connection to system
"""
from enum import Enum
import asyncio
import json
import os
import random
import pandas
import numpy as np

import talib
from talib import stream

from utils.strategy_helpers import Strategy, create_streamer


class Signal(Enum):
    """Signal generated"""
    NULL = 0
    LONG = 1
    SHORT = 2


df = pandas.DataFrame()
loop = asyncio.get_event_loop()
RABBIT_URI = os.getenv("RABBIT_URI", "amqp://guest:guest@localhost/")
on_stream = create_streamer(loop, RABBIT_URI)
alpha = Strategy("alpha", loop)
alpha.overlap, alpha.sigGenerated, alpha.signal = False, False, Signal.NULL
alpha.eMAFast, alpha.eMASlow, alpha.aDXInterval, alpha.orderPrice, alpha.sl, alpha.tp, secLatestEMAFast, secLatestEMASlow = 10, 25, 14, 0, 0, 0, 0, 0

# Checks entry after signal is generated


def checkEntry(data: dict):
    if (alpha.signal == Signal.LONG) and (data['m'] < data['b']):
        alpha.orderPrice = data['m']
        alpha.sl = alpha.orderPrice-100
        alpha.tp = alpha.orderPrice+150
        print("entered long at " + str(data['t']) + " with entry price : " +
              str(alpha.orderPrice) + " exit at sl = " + str(alpha.sl) + " or tp = " + str(alpha.tp))
        alpha.signal = Signal.NULL
        return True
    elif (alpha.signal == Signal.SHORT) and (data['m'] > data['a']):
        alpha.orderPrice = data['m']
        alpha.sl = alpha.orderPrice+100
        alpha.tp = alpha.orderPrice-150
        print("entered short at " + str(data['t']) + " with entry price : " +
              str(alpha.orderPrice) + " exit at sl = " + str(alpha.sl) + " or tp = " + str(alpha.tp))
        alpha.signal = Signal.NULL
        return True
    return False

# Checks exit


def checkExit(data: dict):
    if (alpha.sl < alpha.orderPrice) and ((alpha.sl >= data['m']) or (alpha.tp <= data['m'])):
        # Do exit action
        print("exited LONG at " + str(data['t']) +
              " with mark price : " + str(data['m']))
        if (alpha.sl >= data['m']):
            print("exit at sl")
            print("lose")
        elif (alpha.tp <= data['m']):
            print("exit at tp")
            print("win")
        return True
    elif (alpha.sl > alpha.orderPrice) and ((alpha.sl <= data['m']) or (alpha.tp >= data['m'])):
        # Do exit action
        print("exited SHORT at " + str(data['t']) +
              " with mark price : " + str(data['m']))
        if (alpha.sl <= data['m']):
            print("exit at sl")
            print("lose")
        elif (alpha.tp >= data['m']):
            print("exit at tp")
            print("win")
        return True
    return False

# Generates trade signals


def genSig(data: dict):

    global df, secLatestEMAFast, secLatestEMASlow
    df = df.append(data, ignore_index=True)

    latestEMAFast = stream.EMA(df['c'], timeperiod=alpha.eMAFast)
    latestEMASlow = stream.EMA(df['c'], timeperiod=alpha.eMASlow)
    latestADX = stream.ADX(df['h'], df['l'], df['c'],
                           timeperiod=alpha.aDXInterval)

    if latestEMAFast > latestEMASlow and secLatestEMAFast < secLatestEMASlow and latestADX < 40 and latestADX > 30:
        alpha.signal = Signal.SHORT
        alpha.sigGenerated = True
    elif latestEMAFast < latestEMASlow and secLatestEMAFast > secLatestEMASlow and latestADX < 40 and latestADX > 30:
        alpha.signal = Signal.LONG
        alpha.sigGenerated = True

    secLatestEMAFast, secLatestEMASlow = latestEMAFast, latestEMASlow

    # reset df to 500 candles for memory management
    if len(df.index) > 5000:
        df = df.iloc[500:]


@on_stream(topic="crypto.futures.tick.btcusdt")
async def on_tick(data: dict):
    """Process data every tick"""
    if alpha.sigGenerated and not alpha.inPosition:
        if checkEntry(data):
            alpha.inPosition = True
            alpha.sigGenerated = False
    elif alpha.inPosition:
        if checkExit(data):
            alpha.inPosition = False


@on_stream(topic="crypto.futures.ohlc.btcusdt")
async def on_candle(data: dict):
    """Process data every candle"""
    genSig(data)


async def main():
    loop.create_task(on_tick)
    loop.create_task(on_candle)


if __name__ == "__main__":
    loop.create_task(main())
    loop.run_forever()
