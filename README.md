# BTC Futures Strategy

## Installation

```bash
pip3 install -r requirements.txt
```

## Running

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

## Development

```bash
pip3 install pip-tools
pip-compile requirements.in > requirements.txt
```


