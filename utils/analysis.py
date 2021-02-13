#!/usr/bin/env python3
# coding: utf-8
"""
:author: pk13055
:brief: analyse order data from the backtest
"""
import argparse
import os

import pandas as pd


def collect_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, default=os.path.join("data", "orders.csv"),
                        help="Order output")
    args = parser.parse_args()
    return args

def generate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Generate overview metrics of the performance"""
    table = []

    longs = df.trade_type == "LONG"
    shorts = ~longs

    # general metrics
    table.append([
        "Trades",
        df.shape[0],
        longs.sum(),
        shorts.sum(),
    ])
    table.append([
        "WR",
        df.status.mean(),
        df.status.loc[longs].mean(),
        df.status.loc[shorts].mean(),
    ])

    # trade duration
    duration = df.exit_time - df.entry_time
    table.append([
        "Duration",
        duration.mean(),
        duration.loc[longs].mean(),
        duration.loc[shorts].mean(),
    ])

    # estimated profits + slippage
    if 'profit' in df.columns:
        profits = df.profit
    else:
        avg_exit = (df.exit_high + df.exit_low) / 2
        profits = avg_exit - df.entry_price
        profits.loc[shorts] *= -1  # opposite movement for shorts
    table.append([
        "Profit",
        profits.sum(),
        profits.loc[longs].sum(),
        profits.loc[shorts].sum(),
    ])
    table.append([
        "Drawdown",
        profits.min(),
        profits.loc[longs].min(),
        profits.loc[shorts].min(),
    ])
    table.append([
        "Max Profit",
        profits.max(),
        profits.loc[longs].max(),
        profits.loc[shorts].max(),
    ])

    table = pd.DataFrame(table, columns=["metric", "overall", "long", "short"]).set_index("metric")
    return table


def plot_trades(orders: pd.DataFrame) -> None:
    """Plot trades given the order timestamps"""
    raise NotImplementedError()


def main():
    args = collect_args()
    df = pd.read_csv(args.input, parse_dates=[0, 1])
    print(f"trading from {df.entry_time.min()} to {df.exit_time.max()} ({df.exit_time.max() - df.entry_time.min()})")
    metrics = generate_metrics(df)
    pd.set_option('display.max_columns', None)
    print(metrics.T.head(100))
    # TODO implement plotting of trades
    # plot_trades(df)


if __name__ == "__main__":
    main()

