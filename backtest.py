#!/usr/bin/env python3
# coding: utf-8
"""
    :author: mystertech
    :brief: BTC futures strategy
"""
import argparse
from datetime import time

import pandas as pd
import talib


def collect_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, default="binance_data.csv",
                        help="OHLCV Data")
    parser.add_argument("-o", "--output", type=str, default="orders.csv",
                        help="Order information data")

    parser.add_argument("--sl", type=int, default=100,
                        help="Stoploss")
    parser.add_argument("--rr", type=float, default=1.5,
                        help="Risk reward ratio 1:X")

    parser.add_argument("-s", "--ema_slow", type=int,
                        default=25, help="EMA Slow")
    parser.add_argument("-f", "--ema_fast", type=int,
                        default=10, help="EMA Fast")
    parser.add_argument("-a", "--adx", type=int,
                        default=14, help="ADX period")

    args = parser.parse_args()
    return args


def main():
    args = collect_args()
    df = pd.read_csv(args.input, parse_dates=[0])

    df['emaFast'] = talib.EMA(df.Close, timeperiod=args.ema_fast)
    df['emaSlow'] = talib.EMA(df.Close, timeperiod=args.ema_slow)
    df['adx'] = talib.ADX(df.High, df.Low,
                          df.Close, timeperiod=args.adx)

    signal, status, inPosition, exited = None, None, False, False
    sl, tp, buyPrice, win, loss = 0, 0, 0, 0, 0
    orders = []
    orderTime = df.iloc[0].Timestamp
    n_candles, n_fields = df.shape

    for i in range(n_candles):
        print(
            f"Processing | W:{win}|L:{loss} [{i + 1}/{n_candles}]", end="\r", flush=True)

        # check entry signal condition
        if signal is None and not inPosition:
            if df.at[i, 'emaFast'] > df.at[i, 'emaSlow'] and \
                    df.at[i - 1, 'emaFast'] < df.at[i - 1, 'emaSlow'] and \
                    df.at[i, 'adx'] < 40 and df.at[i, 'adx'] > 30:
                signal = "Short"
            elif df.at[i, 'emaFast'] < df.at[i, 'emaSlow'] and \
                    df.at[i - 1, 'emaFast'] > df.at[i - 1, 'emaSlow'] and \
                    df.at[i, 'adx'] < 40 and df.at[i, 'adx'] > 30:
                signal = "Long"

        # enter if signal
        elif not inPosition:
            buyPrice = (df.at[i, 'Close'] + df.at[i, 'Open']) / 2
            inPosition = True
            orderTime = df.at[i, 'Timestamp']
            if signal == "Long":
                tp = buyPrice + (args.sl * args.rr)
                sl = buyPrice - args.sl
            elif signal == "Short":
                tp = buyPrice - (args.sl * args.rr)
                sl = buyPrice + args.sl

        # wait for exit condition
        else:
            if signal == "Long":
                if tp <= df.at[i, 'High']:
                    status = 'W'
                    exited = True
                elif sl >= df.at[i, 'Low']:
                    status = 'L'
                    exited = True
            elif signal == "Short":
                if tp >= df.at[i, 'Low']:
                    status = 'W'
                    exited = True
                elif sl <= df.at[i, 'High']:
                    status = 'L'
                    exited = True

            # reset variables for next trade if exited
            if exited:
                win += status == 'W'
                loss += status == 'L'
                orders.append({
                    'Timestamp': orderTime,
                    'BuyPrice': buyPrice,
                    'TP': tp,
                    'SL': sl,
                    'W/L': status
                })
                signal, status, inPosition, exited = None, None, False, False
                sl, tp, buyPrice = 0, 0, 0

    total = win + loss
    print(
        f"\nT: {total} :: W: {win} | L: {loss} [{round(win / total * 100, 2)}%]")
    orders = pd.DataFrame(orders)
    orders.to_csv(args.output, float_format="%.3f")


if __name__ == "__main__":
    main()
