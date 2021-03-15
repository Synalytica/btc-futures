#!/usr/bin/env python3
# coding: utf-8
"""
:author: pk13055
:brief: analyse order data from the backtest
"""
import numpy as np
import matplotlib.pyplot as plt
from numpy.lib.stride_tricks import as_strided
import argparse
import os

import pandas as pd


def collect_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, default=os.path.join("data", "orders.csv"),
                        help="Order output")
    args = parser.parse_args()
    return args


def windowed_view(x, window_size):
    """Creat a 2d windowed view of a 1d array.

    `x` must be a 1d numpy array.

    `numpy.lib.stride_tricks.as_strided` is used to create the view.
    The data is not copied.

    Example:

    >>> x = np.array([1, 2, 3, 4, 5, 6])
    >>> windowed_view(x, 3)
    array([[1, 2, 3],
           [2, 3, 4],
           [3, 4, 5],
           [4, 5, 6]])
    """
    y = as_strided(x, shape=(x.size - window_size + 1, window_size),
                   strides=(x.strides[0], x.strides[0]))
    return y


def rolling_max_dd(x, window_size, min_periods=1):
    """Compute the rolling maximum drawdown of `x`.

    `x` must be a 1d numpy array.
    `min_periods` should satisfy `1 <= min_periods <= window_size`.

    Returns an 1d array with length `len(x) - min_periods + 1`.
    """
    if min_periods < window_size:
        pad = np.empty(window_size - min_periods)
        pad.fill(x[0])
        x = np.concatenate((pad, x))
    y = windowed_view(x, window_size)
    running_max_y = np.maximum.accumulate(y, axis=1)
    dd = y - running_max_y
    return dd.min(axis=1)


def max_dd(ser):
    #max2here = pd.expanding_max(ser)
    max2here = ser.cummax()
    dd2here = ser - max2here
    return dd2here.min()


def rmdd(s):
    window_length = 10

    rolling_dd = s.rolling(window_length).apply(max_dd)
    df = pd.concat([s, rolling_dd], axis=1)
    df.columns = ['s', 'rol_dd_%d' % window_length]
    df.plot(linewidth=3, alpha=0.4)

    my_rmdd = rolling_max_dd(s.values, window_length, min_periods=1)
    plt.plot(my_rmdd, 'g.')
    plt.legend(["balance", "rolling drawdown",
                "rolling max drawdown"], loc="upper left")
    # plt.show()
    # TODO implement plotting of rmdd
    return my_rmdd


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
        "Balance",
        profits.cumsum().iloc[-1] + 1000,
        profits.loc[longs].cumsum().iloc[-1] + 1000,
        profits.loc[shorts].cumsum().iloc[-1] + 1000,
    ])
    table.append([
        "Max Profit",
        profits.cummax().max(),
        profits.loc[longs].cummax().max(),
        profits.loc[shorts].cummax().max(),
    ])
    df.entry_time = df.entry_time.round("T").map(lambda x: int(x.timestamp()))
    df.exit_time = df.exit_time.round("T").map(lambda x: int(x.timestamp()))
    df['avg_time'] = (df.entry_time + df.exit_time) / 2

    df["balance"] = profits.cumsum() + 1000
    df = df.groupby("avg_time").max(['balance'])
    rdd = rmdd(df.balance)

    df["balance_longs"] = profits.loc[longs].cumsum() + 1000
    df_longs = df.groupby("avg_time").max(["balance_longs"])
    rdd_longs = rmdd(df_longs.balance_longs)

    df["balance_shorts"] = profits.loc[shorts].cumsum() + 1000
    df_shorts = df.groupby("avg_time").max(["balance_shorts"])
    rdd_shorts = rmdd(df_shorts.balance_shorts)

    table.append(["Rolling drawdown",
                  rdd,
                  rdd_longs,
                  rdd_shorts])
    table = pd.DataFrame(
        table, columns=["metric", "overall", "long", "short"]).set_index("metric")
    return table


def plot_trades(orders: pd.DataFrame) -> None:
    """Plot trades given the order timestamps"""
    raise NotImplementedError()


def main():
    args = collect_args()
    df = pd.read_csv(args.input, parse_dates=[0, 1])
    print(
        f"trading from {df.entry_time.min()} to {df.exit_time.max()} ({df.exit_time.max() - df.entry_time.min()})")
    metrics = generate_metrics(df)
    pd.set_option('display.max_columns', None)
    print(metrics.T.head(100))
    # TODO implement plotting of trades
    # plot_trades(df)


if __name__ == "__main__":
    main()
