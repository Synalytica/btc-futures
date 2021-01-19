# BTC Futures Strategy

## Installation

```bash
pip3 install -r requirements.txt
```

## Running


### `backtest.py`

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

### `analysis.py`

```bash
usage: analysis.py [-h] [-i INPUT]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        Order output 
```

- Example: `./analysis.py -i data/orders.csv`

### `live.py`

- Add `$BINANCE_API_KEY` and `$BINANCE_API_SECRET` to your environment

```bash
BINANCE_API_KEY=your-api-key BINANCE_API_SECRET=your-api-secret ./live.py
```

## Development

```bash
pip3 install pip-tools
pip-compile requirements.in > requirements.txt
```

