from enum import Enum


class StreamType(str, Enum):
    """Types of streams to fetch data from"""

    MARK_PRICE = "btcusdt@markPrice@1s"
    LAST_PRICE = "btcusdt@bookTicker"

    def __repr__(self):
        return self.value


class Stage(str, Enum):
    """Stage of the Strategy"""

    BACKTEST = "backtest"
    OPTIMIZE = "optimize"
    PAPER = "paper"
    LIVE = "live"
    ARCHIVE = "archive"
    LIQUIDATE = "liquidate"

    def __repr__(self):
        return self.value


class StrategyType(str, Enum):
    """Type of the Strategy"""

    TREND = "trend"
    BREAKOUT = "breakout"
    REVERSAL = "reversal"
    SWING = "swing"

    def __repr__(self):
        return self.value


class MessageCounter(int, Enum):
    """Handles Message Counts"""

    RECEIVE = -1
    ADD = 1
    RESET = 0

    def __repr__(self):
        return self.value
