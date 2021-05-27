#!/usr/bin/env python3
# coding: utf-8
"""
    :author: pk13055,mystertech,shreygupta2809
    :brief: basic strategy to outline connection to system
"""
import argparse
import asyncio

from utils.enums import Stage
from utils.strategy_helpers import Strategy


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
    parser.add_argument(
        "--stage", type=Stage, default=Stage.LIVE, help="Execution Stage"
    )
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
        "EMACross-ADX",
        args.stage.upper(),
        loop=loop,
        start_date=args.start,
        end_date=args.end,
    )
    loop.create_task(ema_adx.run())


if __name__ == "__main__":
    loop.create_task(main())
    loop.run_forever()
