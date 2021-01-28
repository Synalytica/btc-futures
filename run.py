#!/usr/bin/env python3
from asyncio import get_event_loop
import os
import sys

from binance.exceptions import BinanceAPIException

from engine import Engine


async def main():
    API_KEY = os.getenv("BINANCE_API_KEY", None)
    API_SECRET = os.getenv("BINANCE_API_SECRET", None)
    engine = Engine(API_KEY, API_SECRET)
    await engine.run()


if __name__ == "__main__":
    try:
        get_event_loop().run_until_complete(main())
    except BinanceAPIException as e:
        sys.stderr.write(f"[error] Binance: {e}\n")
