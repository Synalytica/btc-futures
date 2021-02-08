#!/usr/bin/env python3
# coding: utf-8
"""
    :author: pk13055
    :brief: parse trade report for live statistics

"""
import argparse
import re

import pandas as pd


float_pattern = r"[-+]?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?"
num_pattern = r"[-+]?\d+"


def collect_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, default="trades.txt",
                        help="File dump of trade log")
    parser.add_argument("-o", "--output", type=str, default="orders.csv",
                        help="Output file to store stastics")
    parser.add_argument("-b", "--balance", type=int, default=500,
                        help="starting balance for simulation")
    parser.add_argument("-r", "--risk", type=float, default=0.2,
                        help="capital lock in % (0-1) per trade")
    args = parser.parse_args()
    return args


def dump_trades(trades: dict, output_path: str) -> None:
    """Dump trades to file"""

    cols = ["entry_time", "exit_time", "trade_type", "entry_price",
            "sl", "tp", "exit_price", "status", "profit"]
    print(trades.head(100))
    trades[cols].to_csv(output_path, index=False)


def calc_profits(trades: pd.DataFrame, balance: int, risk: float) -> pd.DataFrame:
    """Calculate the profits generated"""
    print(f"starting with {balance} (using {risk * 100}% margin)")
    total = balance
    profits, deltas = [], []
    for idx, row in trades.iterrows():
        original = total
        base_price = total * risk
        btc_total = base_price * 125 / row.entry_price
        price_delta = btc_total * row.delta
        total += price_delta
        profits.append(price_delta)
        deltas.append(equity_delta := (total - original) / original * 100)
        print(row.status, total, f"{equity_delta: .3f}%")
    trades.loc[:, "profit"] = profits
    trades.loc[:, "equity_delta"] = deltas
    return trades


def generate_trades(input: str) -> dict:
    """Generate cleaned trades"""
    raw = open(input).read().splitlines()
    _entry_rows, _exits = map(lambda x: x.split("|"), raw[::2]), raw[1::2]
    _entries, _limits = zip(*_entry_rows)

    entry_pattern = r"\[(?P<position>\S+)\]\[(?P<trade_type>\S+)\] (?P<entry_time>num-num-num num:num:float) @ (?P<entry_price>float)"
    entry_pattern = entry_pattern.replace(
        r"num", num_pattern).replace(r"float", float_pattern)
    entries = map(lambda entry: re.search(
        entry_pattern, entry).groupdict(), _entries)

    limits_pattern = r"SL: +(?P<sl>float) :: TP: +(?P<tp>float)"
    limits_pattern = limits_pattern.replace(r"float", float_pattern)
    limits = map(lambda lim: re.search(
        limits_pattern, lim).groupdict(), _limits)

    exit_pattern = r"\[(?P<position>\S+)\]\[(?P<trade_type>\S+)\] (?P<exit_time>num-num-num num:num:float) @ (?P<exit_price>float) \[(?P<status>\S+)\]"
    exit_pattern = exit_pattern.replace(
        r"num", num_pattern).replace(r"float", float_pattern)
    exits = map(lambda exit: re.search(exit_pattern, exit).groupdict(), _exits)

    dataset = [
        {**entry, **lim, **exit} for entry, lim, exit in zip(entries, limits, exits)
    ]
    data = pd.DataFrame.from_dict(dataset)
    return data


def main():
    args = collect_args()
    trades = generate_trades(args.input)

    # format trade df
    del trades['position']
    trades.entry_time = pd.to_datetime(trades.entry_time)
    trades.exit_time = pd.to_datetime(trades.exit_time)
    trades.entry_price = pd.to_numeric(trades.entry_price)
    trades.exit_price = pd.to_numeric(trades.exit_price)
    trades.status = trades.status.map(lambda st: int(st == "WIN"))
    trades.sl = pd.to_numeric(trades.sl)
    trades.tp = pd.to_numeric(trades.tp)
    trades['delta'] = trades.exit_price - trades.entry_price
    trades.loc[trades.trade_type == "SHORT", "delta"] *= -1

    dataset = calc_profits(trades, args.balance, args.risk)
    dump_trades(dataset, args.output)


if __name__ == "__main__":
    main()
