from enum import Enum


class StreamType(str, Enum):
    """Types of streams to fetch data from"""
    MARK_PRICE = "btcusdt@markPrice@1s"
    LAST_PRICE = "btcusdt@bookTicker"

    def __repr__(self):
        return self.value
