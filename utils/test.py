#!/usr/bin/env python3
# coding: utf-8
"""
    :author: pk13055
    :brief: script to test local database instance
    :usage: $ DATABASE_URI=postgresql://username:password@host:port/database ./test.py
"""
import datetime
import os
import random

import asyncio
import asyncpg


async def main():
    DATABASE_URI = os.getenv("DATABASE_URI", 'postgresql://postgres@localhost/test')
    async with asyncpg.create_pool(DATABASE_URI, command_timeout=60) as pool:
        async with pool.acquire() as conn:
            # insert test row
            row = [datetime.datetime.now()] + [100 * random.random() for _ in range(5)]
            await conn.execute('''
                INSERT into ohlc(timestamp, open, high, low, close, volume) VALUES($1, $2, $3, $4, $5, $6)
            ''', *row)
            async with conn.transaction():
                async for row in conn.cursor('''
                                             SELECT * from ohlc
                                             ORDER BY timestamp
                                             ASC limit 15;
                                             '''):
                    # NOTE: rows will not be ordered
                    print(row)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
