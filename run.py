#!/usr/bin/env python3
from asyncio import get_event_loop
import os
import multiprocessing as mp
import sys

from binance.exceptions import BinanceAPIException

from crypto.stream import Stream
from crypto.warehouse import Warehouse


def start_stream():
    """Initialize the streaming of data from Binance"""
    try:
        loop = get_event_loop()
        API_KEY = os.getenv("BINANCE_API_KEY", "change-this-key")
        API_SECRET = os.getenv("BINANCE_API_SECRET", "change-this-secret")
        stream = Stream(("btc", "usdt"), API_KEY, API_SECRET)
        loop.create_task(stream.run())
        loop.run_forever()
    except BinanceAPIException as e:
        sys.stderr.write(f"[error] Stream: {e}\n")


def start_warehouse():
    """Initialize the dumping of data to db"""
    try:
        loop = get_event_loop()
        warehouse = Warehouse()
        loop.create_task(warehouse.run())
        loop.run_forever()
    except Exception as e:
        sys.stderr.write(f"[error] Warehouse: {e}\n")


def main():
    """Declare and start all relevant processes"""

    stream_proc = mp.Process(target=start_stream, name='STREAM', daemon=True)
    warehouse_proc = mp.Process(
        target=start_warehouse, name='WAREHOUSE', daemon=True)

    # NOTE: add all defined processes here
    processes = [
        warehouse_proc,
        stream_proc,
    ]

    # start all the processes
    [(f"[{idx}/{len(processes)}] Process {process.name} starting ...", process.start())
        for idx, process in enumerate(processes)]

    # wait for processess to all exit before quitting the main program
    [_.join() for _ in processes]


if __name__ == "__main__":
    mp.set_start_method('spawn')
    sys.exit(main())
