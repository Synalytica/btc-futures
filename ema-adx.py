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
from datetime import datetime

import numpy as np
import pandas as pd
from talib import stream

from utils.strategy_helpers import Strategy, create_streamer


args = None
loop = asyncio.get_event_loop()


def collect_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ema_fast", type=int, default=10, help="Fast moving ema period"
    )
    parser.add_argument(
        "--ema_slow", type=int, default=25, help="Slow moving ema period"
    )
    parser.add_argument("--adx", type=int, default=14, help="ADX period")
    parser.add_argument("--sl", type=float, default=100, help="ADX period")
    parser.add_argument("--tp", type=float, default=150, help="ADX period")
    parser.add_argument("--stage", default="L", help="Execution Stage")
    parser.add_argument(
        "--start",
        default="",
        help="Start date for Backtest [YYYY/MM/DD]",
    )
    parser.add_argument(
        "--end",
        default="",
        help="End date for Backtest [YYYY/MM/DD]",
    )
    args = parser.parse_args()
    return args


async def main():
    global args
    args = collect_args()
    ema_adx = Strategy(
        "EMACross-ADX", args.stage.upper(), start_date=args.start, end_date=args.end
    )
    await ema_adx.run()


if __name__ == "__main__":
    loop.create_task(main())
    loop.run_forever()
