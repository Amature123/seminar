"""
Python UDFs cho Flink SQL.

Đây là phần "khai thác Flink" mà SQL thuần không làm được: các chỉ báo có tính
ĐỆ QUY (EMA, RSI theo Wilder) không thể biểu diễn bằng OVER window built-in.
Ta gom N giá close gần nhất thành ARRAY (bằng ARRAY_AGG trong một OVER window có
biên), rồi để UDF scalar tính chỉ báo trên mảng đó. Cách này chỉ dùng UDF scalar
(rất ổn định trong PyFlink) thay vì UDAF-over-window (hỗ trợ hạn chế).
"""
from typing import List, Optional

from pyflink.table import DataTypes
from pyflink.table.udf import udf


@udf(result_type=DataTypes.DOUBLE())
def ema(prices: List[float], span: int) -> Optional[float]:
    """Exponential Moving Average. prices đã được sắp theo thời gian tăng dần."""
    if not prices or span is None or span <= 0:
        return None
    vals = [float(p) for p in prices if p is not None]
    if not vals:
        return None
    alpha = 2.0 / (span + 1.0)
    e = vals[0]
    for p in vals[1:]:
        e = alpha * p + (1.0 - alpha) * e
    return float(e)


@udf(result_type=DataTypes.DOUBLE())
def rsi(prices: List[float], period: int) -> Optional[float]:
    """Relative Strength Index (Wilder's smoothing)."""
    if not prices or period is None or period <= 0:
        return None
    vals = [float(p) for p in prices if p is not None]
    if len(vals) < period + 1:
        return None

    gains, losses = [], []
    for prev, cur in zip(vals[:-1], vals[1:]):
        change = cur - prev
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))

    # Seed bằng trung bình đơn của `period` giá trị đầu
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    # Wilder smoothing cho phần còn lại
    for g, l in zip(gains[period:], losses[period:]):
        avg_gain = (avg_gain * (period - 1) + g) / period
        avg_loss = (avg_loss * (period - 1) + l) / period

    if avg_loss == 0.0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - 100.0 / (1.0 + rs))


@udf(result_type=DataTypes.DOUBLE())
def macd(prices: List[float], fast: int, slow: int) -> Optional[float]:
    """MACD line = EMA(fast) - EMA(slow)."""
    fast_ema = _ema_value(prices, fast)
    slow_ema = _ema_value(prices, slow)
    if fast_ema is None or slow_ema is None:
        return None
    return float(fast_ema - slow_ema)


def _ema_value(prices, span):
    if not prices or span is None or span <= 0:
        return None
    vals = [float(p) for p in prices if p is not None]
    if not vals:
        return None
    alpha = 2.0 / (span + 1.0)
    e = vals[0]
    for p in vals[1:]:
        e = alpha * p + (1.0 - alpha) * e
    return e
