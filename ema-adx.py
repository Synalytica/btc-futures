#!/usr/bin/env python3
# coding: utf-8
"""
    :author: pk13055, mystertech, shreygupta2809
    :brief: basic strategy to outline connection to system
"""
import asyncio
import sys

from utils.config_parser import collect_configs
from utils.strategy_helpers import Strategy


args = None
sections = None
loop = asyncio.get_event_loop()


async def main():
    ema_adx = Strategy(
        stage=args.stage.upper(),
        loop=loop,
        params=sections,
    )
    loop.create_task(ema_adx.run())


if __name__ == "__main__":
    sections, args = collect_configs()
    loop.create_task(main())
    loop.run_forever()
