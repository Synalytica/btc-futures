# BTC Futures Strategy

![lint-status](https://github.com/Synalytica/btc-futures/workflows/Python%20application/badge.svg)

## Installation & Setup

### Dependencies

- Install the script dependencies

```bash
pip3 install -r requirements.txt
```

### Database

- Populate the environment with necessary variables (as outlined in
  `env.example`)

- Run the docker containers

```bash
docker-compose up
```

- Test your database

```bash
./utils/test.py
```

## Running

### `run.py`

- Populate the environment with necessary variables (as outlined in
  `env.example`)

- Make sure the database is running (refer to above section)

```bash
./run.py
```

### `./utils/gen_chart_data.py`

```bash
usage: gen_chart_data.py [-h] [-i INPUT] [-c CANDLES] [-o OUTPUT] [--asset_class ASSET CLASS] [-t TYPE] [-a ASSET] [-f FREQUENCY]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        Order data
  -c CANDLES, --candles CANDLES
                        Candle data dump
  -o OUTPUT, --output OUTPUT
                        Output charting json
  --asset_class ASSET_CLASS
                        Asset Class ["crypto", "stock", "forex", "commodities"]
  -t TYPE, --type TYPE
                        Type of Asset ["futures", "spot", "margin"]
  -a ASSET, --asset ASSET
                        Asset
  -f FREQUENCY, --frequency FREQUENCY
                        Candle frequency $num["m", "h", "d"]
```

- Example: `./gen_chart_data.py -i ../data/orders.csv -c ../data/candles.csv -o ../data/data.json --asset_class crypto -t futures -a btcusdt -f 1m `
- This generated `data.json` can be placed in
  [`$PROJECT_DIR`](https://github.com/Synalytica/visualization-engine)`/public/`
  to visualize using trading view.

### `utils/summary.py`

- First, copy the trade output from the strategy into a file, say `trades.txt`
- Next run the script with a desired balance and risk tolerance per trade.

```bash
./summary.py -i ../data/trades.txt -b 500 -r 0.1  # balance $500, risk 10%
```

- _Optionally_, pass the generated `orders.csv` into the `analysis.py` script for
  aggregated stats.

### `./utils/backtest.py`

```bash
usage: backtest.py [-h] [-i INPUT] [-o OUTPUT] [-s EMA_SLOW] [-f EMA_FAST]
                   [-a ADX]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        OHLCV Data
  -o OUTPUT, --output OUTPUT
                        Order information data
  -s EMA_SLOW, --ema_slow EMA_SLOW
                        EMA Slow
  -f EMA_FAST, --ema_fast EMA_FAST
                        EMA Fast
  -a ADX, --adx ADX     ADX period
```

- Example: `./backtest.py -i data/binance_data.csv -o data/orders.csv`

### `utils/analysis.py`

```bash
usage: analysis.py [-h] [-i INPUT]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        Order output
```

- Example: `./analysis.py -i data/orders.csv`

### `ema-adx.py`

```bash
usage: ema-adx.py [-h] [-c config] [--stage STAGE]

optional arguments:
  -h, --help           
                       show this help message and exit
  -c CONFIG, --config CONFIG           
                       Path to Config file containing params
  -s STAGE, --stage STAGE        
                       Execution Stage ["backtest", "optimize", "live", "paper", "archive", "liquidate"]
```

- Example: `./ema-adx.py -c configs/default.cfg --stage backtest`

## Development

```bash
pip3 install pip-tools
pip-compile requirements.in > requirements.txt
```
