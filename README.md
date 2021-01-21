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

- Run the docker container

```bash
docker run --rm -d -e POSTGRES_USER=$POSTGRES_USER -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD -e POSTGRES_DB=$POSTGRES_DB -v $PWD/data/postgres:/var/lib/postgresql/data -v $PWD/config/db/:/docker-entrypoint-initdb.d/ --name timescaledb -p 5432:5432 timescale/timescaledb:2.0.0-pg12
```

- Test your database

```bash
./utils/test.py
```

## Running

### `live.py`

- Populate the environment with necessary variables (as outlined in
  `env.example`)

```bash
./live.py
```
 
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

### `utils/analysis.py`

```bash
usage: analysis.py [-h] [-i INPUT]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        Order output 
```

- Example: `./analysis.py -i data/orders.csv`

## Development

```bash
pip3 install pip-tools
pip-compile requirements.in > requirements.txt
```


