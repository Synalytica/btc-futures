#!/usr/bin/env python3
# coding: utf-8
"""
    :author: pk13055
    :brief: basic strategy to outline connection to system
"""
import asyncio
import json
import os
import random

import talib as ta

from utils.strategy_helpers import Strategy, create_streamer


loop = asyncio.get_event_loop()
RABBIT_URI = os.getenv("RABBIT_URI", "amqp://guest:guest@localhost/")
on_stream = create_streamer(loop, RABBIT_URI)
EMACrossADX = Strategy("EMACross+ADX", loop)


@on_stream(topic="crypto.futures.tick.btcusdt")
async def on_tick(data: dict):
    """Process data every tick"""
    if EMACrossADX.inPosition:
        print(data)
        if random.random() > 0.5:
            EMACrossADX.inPosition = False


@on_stream(topic="crypto.futures.ohlc.btcusdt")
async def on_candle(data: dict):
    """Process data every candle"""
    print(data)
    if random.random() < 0.5:
        EMACrossADX.inPosition = True


async def main():
    loop.create_task(on_tick)
    loop.create_task(on_candle)


if __name__ == "__main__":
    loop.create_task(main())
    loop.run_forever()
