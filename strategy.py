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

import talib as ta

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
alpha.eMAFast, alpha.eMASlow, alpha.aDXInterval = 10, 25, 14


def checkEntry(data: dict):
    if (alpha.signal == Signal.LONG) and (data.m < data.b):
        alpha.orderprice = data.m
        alpha.sl = alpha.orderprice-100
        alpha.tp = alpha.orderprice+150
        print("entered long at " + str(data.t) + " with mark price : " +
              str(data.m) + " exit at sl = " + str(alpha.sl) + " or tp = " + str(alpha.tp))
        return True
    elif (alpha.signal == Signal.SHORT) and (data.m > data.a):
        alpha.orderprice = data.m
        alpha.sl = alpha.orderprice+100
        alpha.tp = alpha.orderprice-150
        print("entered long at " + str(data.t) + " with mark price : " +
              str(data.m) + " exit at sl = " + str(alpha.sl) + " or tp = " + str(alpha.tp))
        return True
    return False


def checkExit(data: dict):
    if (alpha.sl < alpha.orderPrice) and ((alpha.sl >= data.m) or (alpha.tp <= data.m)):
        # Do exit action
        print("exited LONG at " + str(data.t) +
              " with mark price : " + str(data.m))
        if (alpha.sl >= data.m):
            print("win")
        elif (alpha.tp <= data.m):
            print("lose")
    elif (alpha.sl > alpha.orderPrice) and ((alpha.sl <= data.m) or (alpha.tp >= data.m)):
        # Do exit action
        print("exited SHORT at " + str(data.t) +
              " with mark price : " + str(data.m))
        if (alpha.sl <= data.m):
            print("win")
        elif (alpha.tp >= data.m):
            print("lose")


def genSig(data: dict):
    global df
    df = df.append(data, ignore_index=True)
    df['emaFast'] = ta.EMA(df.Close, timeperiod=alpha.eMAFast)
    df['emaSlow'] = ta.EMA(df.Close, timeperiod=alpha.eMASlow)
    df['adx'] = ta.ADX(df.High, df.Low,
                       df.Close, timeperiod=alpha.aDXInterval)

    if df.at[-1, 'emaFast'] > df.at[-1, 'emaSlow'] and df.at[-2, 'emaFast'] < df.at[-2, 'emaSlow'] and df.at[-1, 'adx'] < 40 and df.at[-1, 'adx'] > 30:
        alpha.signal = Signal.SHORT
        alpha.sigGenerated = True

    elif df.at[-1, 'emaFast'] < df.at[-1, 'emaSlow'] and df.at[-2, 'emaFast'] > df.at[-2, 'emaSlow'] and df.at[-1, 'adx'] < 40 and df.at[-1, 'adx'] > 30:
        alpha.signal = Signal.LONG
        alpha.sigGenerated = True

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
    print(data)
    genSig(data)


async def main():
    loop.create_task(on_tick)
    loop.create_task(on_candle)


if __name__ == "__main__":
    loop.create_task(main())
    loop.run_forever()
