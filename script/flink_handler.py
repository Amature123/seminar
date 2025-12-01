from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.typeinfo import Types
from pyflink.common.time import Duration
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction, KeyedProcessFunction
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows, Time
from datetime import datetime
import json
import logging
from collections import deque
from pyflink.common import Configuration
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class StockTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_timestamp):
        data = json.loads(event)
        dt = datetime.strptime(data["time"], "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp() * 1000)


# ============================================================================
# USE CASE 1: TIME-BASED AGGREGATIONS (OHLC Resampling) - REALTIME
# ============================================================================
class OHLCRealtimeProcessor(KeyedProcessFunction):

    def open(self, runtime_context):
        # MapState: timeframe -> window data
        descriptor = MapStateDescriptor(
            "ohlc_state",
            Types.STRING(),   # key: '1min', '5min', ...
            Types.PICKLED_BYTE_ARRAY()  # value: dictionary
        )
        self.state = runtime_context.get_map_state(descriptor)

    def process_element(self, value, ctx):
        data = json.loads(value) if isinstance(value, str) else value

        symbol = data['symbol']
        price = float(data['close'])
        high = float(data['high'])
        low = float(data['low'])
        volume = float(data.get('volume', 0))

        current_time = ctx.timestamp()

        timeframes = {
            '1min': 60 * 1000,
            '5min': 5 * 60 * 1000,
            '15min': 15 * 60 * 1000
        }

        for tf_name, tf_ms in timeframes.items():
            window_start = (current_time // tf_ms) * tf_ms

            # load state
            window = self.state.get(tf_name)

            # initialize
            if window is None or window['start_time'] != window_start:
                window = {
                    'open': price,
                    'high': high,
                    'low': low,
                    'close': price,
                    'volume': volume,
                    'start_time': window_start,
                    'count': 1
                }
            else:
                window['high'] = max(window['high'], high)
                window['low'] = min(window['low'], low)
                window['close'] = price
                window['volume'] += volume
                window['count'] += 1
            
            # save back
            self.state.put(tf_name, window)

            # output result
            result = {
                'symbol': symbol,
                'timeframe': tf_name,
                'open': round(window['open'], 2),
                'high': round(window['high'], 2),
                'low': round(window['low'], 2),
                'close': round(window['close'], 2),
                'volume': round(window['volume'], 2),
                'tick_count': window['count'],
                'type': 'ohlc_realtime'
            }

            ctx.output(result) 
# ============================================================================
# USE CASE 2: TECHNICAL INDICATORS - OPTIMIZED FOR REALTIME
# ============================================================================
class TechnicalIndicatorProcessor(KeyedProcessFunction):
    """
    Calculate SMA, EMA, RSI in realtime
    Output on EVERY tick with latest values
    """
    
    def __init__(self, sma_period=20, ema_short=12, ema_long=26, rsi_period=14):
        self.sma_period = sma_period
        self.ema_short = ema_short
        self.ema_long = ema_long
        self.rsi_period = rsi_period
    
    def open(self, runtime_context):
        self.price_history = {}
        self.ema_short_value = {}
        self.ema_long_value = {}
        self.prev_price = {}
        self.gains = {}
        self.losses = {}
        self.avg_gain = {}
        self.avg_loss = {}
    
    def process_element(self, value, ctx):
        data = json.loads(value) if isinstance(value, str) else value
        symbol = data['symbol']
        price = float(data['close'])
        
        # Initialize
        if symbol not in self.price_history:
            self.price_history[symbol] = deque(maxlen=self.sma_period)
            self.ema_short_value[symbol] = None
            self.ema_long_value[symbol] = None
            self.prev_price[symbol] = price
            self.gains[symbol] = deque(maxlen=self.rsi_period)
            self.losses[symbol] = deque(maxlen=self.rsi_period)
            self.avg_gain[symbol] = 0
            self.avg_loss[symbol] = 0
        
        self.price_history[symbol].append(price)
        
        # === SMA Calculation ===
        sma = sum(self.price_history[symbol]) / len(self.price_history[symbol])
        
        # === EMA Calculation (incremental) ===
        if self.ema_short_value[symbol] is None:
            self.ema_short_value[symbol] = price
            self.ema_long_value[symbol] = price
        else:
            alpha_short = 2 / (self.ema_short + 1)
            alpha_long = 2 / (self.ema_long + 1)
            self.ema_short_value[symbol] = alpha_short * price + (1 - alpha_short) * self.ema_short_value[symbol]
            self.ema_long_value[symbol] = alpha_long * price + (1 - alpha_long) * self.ema_long_value[symbol]
        
        # === MACD Calculation ===
        macd = self.ema_short_value[symbol] - self.ema_long_value[symbol]
        
        # === RSI Calculation (incremental Wilder's method) ===
        change = price - self.prev_price[symbol]
        gain = max(change, 0)
        loss = abs(min(change, 0))
        
        self.gains[symbol].append(gain)
        self.losses[symbol].append(loss)
        
        # Use Wilder's smoothing method for RSI
        if len(self.gains[symbol]) < self.rsi_period:
            # Initial period: use simple average
            self.avg_gain[symbol] = sum(self.gains[symbol]) / len(self.gains[symbol]) if self.gains[symbol] else 0
            self.avg_loss[symbol] = sum(self.losses[symbol]) / len(self.losses[symbol]) if self.losses[symbol] else 0
        else:
            # After initial period: use Wilder's smoothing
            self.avg_gain[symbol] = (self.avg_gain[symbol] * (self.rsi_period - 1) + gain) / self.rsi_period
            self.avg_loss[symbol] = (self.avg_loss[symbol] * (self.rsi_period - 1) + loss) / self.rsi_period
        
        rsi = 50  # Default
        if self.avg_loss[symbol] > 0:
            rs = self.avg_gain[symbol] / self.avg_loss[symbol]
            rsi = 100 - (100 / (1 + rs))
        elif self.avg_gain[symbol] > 0:
            rsi = 100
        
        self.prev_price[symbol] = price
        
        # === Bollinger Bands ===
        bb_upper = None
        bb_lower = None
        if len(self.price_history[symbol]) >= 20:
            prices = list(self.price_history[symbol])
            std_dev = (sum((p - sma) ** 2 for p in prices) / len(prices)) ** 0.5
            bb_upper = sma + (2 * std_dev)
            bb_lower = sma - (2 * std_dev)
        
        # Output
        result = {
            'symbol': symbol,
            'price': round(price, 2),
            'sma_20': round(sma, 2),
            'ema_12': round(self.ema_short_value[symbol], 2),
            'ema_26': round(self.ema_long_value[symbol], 2),
            'macd': round(macd, 4),
            'rsi_14': round(rsi, 2),
            'bb_upper': round(bb_upper, 2) if bb_upper else None,
            'bb_lower': round(bb_lower, 2) if bb_lower else None,
            'type': 'technical_indicators'
        }
        
        yield json.dumps(result)


# ============================================================================
# USE CASE 3: PATTERN DETECTION
# ============================================================================
class PatternDetectionProcessor(KeyedProcessFunction):
    """Detect candlestick patterns and breakouts"""
    
    def open(self, runtime_context):
        self.candle_history = {}
    
    def process_element(self, value, ctx):
        data = json.loads(value) if isinstance(value, str) else value
        symbol = data['symbol']
        
        if symbol not in self.candle_history:
            self.candle_history[symbol] = deque(maxlen=3)
        
        candle = {
            'open': float(data['open']),
            'high': float(data['high']),
            'low': float(data['low']),
            'close': float(data['close']),
        }
        
        self.candle_history[symbol].append(candle)
        
        patterns = []
        
        if len(self.candle_history[symbol]) >= 1:
            current = self.candle_history[symbol][-1]
            body = abs(current['close'] - current['open'])
            total_range = current['high'] - current['low']
            
            # Doji pattern
            if total_range > 0 and body / total_range < 0.1:
                patterns.append('DOJI')
            
            # Hammer pattern
            lower_shadow = min(current['open'], current['close']) - current['low']
            if total_range > 0 and lower_shadow / total_range > 0.6:
                patterns.append('HAMMER')
        
        if len(self.candle_history[symbol]) >= 2:
            prev = self.candle_history[symbol][-2]
            current = self.candle_history[symbol][-1]
            
            # Bullish Engulfing
            if (prev['close'] < prev['open'] and 
                current['close'] > current['open'] and
                current['close'] > prev['open'] and
                current['open'] < prev['close']):
                patterns.append('BULLISH_ENGULFING')
            
            # Bearish Engulfing
            if (prev['close'] > prev['open'] and 
                current['close'] < current['open'] and
                current['close'] < prev['open'] and
                current['open'] > prev['close']):
                patterns.append('BEARISH_ENGULFING')
        
        if patterns:
            result = {
                'symbol': symbol,
                'patterns': patterns,
                'price': float(data['close']),
                'type': 'pattern_detection'
            }
            yield json.dumps(result)


# ============================================================================
# USE CASE 4: REAL-TIME ALERTS
# ============================================================================
class AlertProcessor(KeyedProcessFunction):
    """Generate alerts for price movements, volume spikes, etc."""
    
    def __init__(self, price_threshold=0.05, volume_multiplier=2.0):
        self.price_threshold = price_threshold  # 5% change
        self.volume_multiplier = volume_multiplier
    
    def open(self, runtime_context):
        self.last_price = {}
        self.avg_volume = {}
        self.volume_history = {}
    
    def process_element(self, value, ctx):
        data = json.loads(value) if isinstance(value, str) else value
        symbol = data['symbol']
        price = float(data['close'])
        volume = float(data.get('volume', 0))
        
        alerts = []
        
        # Price change alert
        if symbol in self.last_price:
            change_pct = abs(price - self.last_price[symbol]) / self.last_price[symbol]
            if change_pct > self.price_threshold:
                alerts.append({
                    'alert_type': 'PRICE_SPIKE',
                    'message': f'Price changed by {change_pct*100:.2f}%',
                    'severity': 'HIGH' if change_pct > 0.1 else 'MEDIUM'
                })
        
        self.last_price[symbol] = price
        
        # Volume spike alert
        if symbol not in self.volume_history:
            self.volume_history[symbol] = deque(maxlen=20)
        
        self.volume_history[symbol].append(volume)
        
        if len(self.volume_history[symbol]) >= 10:
            avg_vol = sum(self.volume_history[symbol]) / len(self.volume_history[symbol])
            if volume > avg_vol * self.volume_multiplier:
                alerts.append({
                    'alert_type': 'VOLUME_SPIKE',
                    'message': f'Volume is {volume/avg_vol:.2f}x average',
                    'severity': 'MEDIUM'
                })
        
        # Output alerts
        if alerts:
            result = {
                'symbol': symbol,
                'price': price,
                'volume': volume,
                'alerts': alerts,
                'type': 'real_time_alerts'
            }
            yield json.dumps(result)


# ============================================================================
# USE CASE 5: MULTI-SYMBOL ANALYSIS - REALTIME
# ============================================================================
class MultiSymbolCorrelationProcessor(KeyedProcessFunction):
    """
    Analyze correlation and relative strength between symbols in realtime
    Process ALL symbols together (not keyed)
    """
    
    def open(self, runtime_context):
        self.symbol_prices = {}  # {symbol: deque of recent prices}
        self.symbol_start_price = {}  # Starting price for % change calculation
        self.last_update_time = {}
    
    def process_element(self, value, ctx):
        data = json.loads(value) if isinstance(value, str) else value
        symbol = data['symbol']
        price = float(data['close'])
        current_time = ctx.timestamp() if ctx.timestamp() else 0
        
        # Initialize
        if symbol not in self.symbol_prices:
            self.symbol_prices[symbol] = deque(maxlen=100)
            self.symbol_start_price[symbol] = price
            self.last_update_time[symbol] = current_time
        
        self.symbol_prices[symbol].append(price)
        self.last_update_time[symbol] = current_time
        
        # Only calculate if we have at least 2 symbols with enough data
        active_symbols = [s for s in self.symbol_prices.keys() if len(self.symbol_prices[s]) >= 20]
        
        if len(active_symbols) >= 2:
            # Calculate current symbol's performance
            symbol_change = (price - self.symbol_start_price[symbol]) / self.symbol_start_price[symbol]
            
            # Calculate market average (exclude current symbol)
            market_changes = []
            for other_symbol in active_symbols:
                if other_symbol != symbol:
                    other_prices = list(self.symbol_prices[other_symbol])
                    if len(other_prices) > 0:
                        other_current = other_prices[-1]
                        other_start = self.symbol_start_price[other_symbol]
                        other_change = (other_current - other_start) / other_start
                        market_changes.append(other_change)
            
            if market_changes:
                avg_market_change = sum(market_changes) / len(market_changes)
                relative_strength = symbol_change - avg_market_change
                
                # Calculate beta (simplified - price correlation with market)
                if len(self.symbol_prices[symbol]) >= 50:
                    symbol_returns = []
                    market_returns = []
                    
                    prices = list(self.symbol_prices[symbol])
                    for i in range(1, min(50, len(prices))):
                        symbol_ret = (prices[i] - prices[i-1]) / prices[i-1]
                        symbol_returns.append(symbol_ret)
                        
                        # Calculate market return (average of other symbols)
                        market_ret_sum = 0
                        count = 0
                        for other in active_symbols:
                            if other != symbol and len(self.symbol_prices[other]) > i:
                                other_prices = list(self.symbol_prices[other])
                                if i < len(other_prices):
                                    ret = (other_prices[i] - other_prices[i-1]) / other_prices[i-1]
                                    market_ret_sum += ret
                                    count += 1
                        if count > 0:
                            market_returns.append(market_ret_sum / count)
                    
                    # Calculate beta (covariance / variance)
                    beta = 1.0
                    if len(symbol_returns) == len(market_returns) and len(market_returns) > 0:
                        mean_symbol = sum(symbol_returns) / len(symbol_returns)
                        mean_market = sum(market_returns) / len(market_returns)
                        
                        covariance = sum((symbol_returns[i] - mean_symbol) * (market_returns[i] - mean_market) 
                                       for i in range(len(symbol_returns))) / len(symbol_returns)
                        variance = sum((r - mean_market) ** 2 for r in market_returns) / len(market_returns)
                        
                        if variance > 0:
                            beta = covariance / variance
                else:
                    beta = 1.0
                
                result = {
                    'symbol': symbol,
                    'price': round(price, 2),
                    'symbol_change_pct': round(symbol_change * 100, 2),
                    'market_avg_change_pct': round(avg_market_change * 100, 2),
                    'relative_strength': round(relative_strength * 100, 2),
                    'beta': round(beta, 2),
                    'market_symbols_count': len(active_symbols) - 1,
                    'type': 'multi_symbol_analysis'
                }
                yield json.dumps(result)


# ============================================================================
# USE CASE 6: RISK METRICS
# ============================================================================
class RiskMetricsProcessor(KeyedProcessFunction):
    """Calculate drawdown, Sharpe ratio, VaR"""
    
    def open(self, runtime_context):
        self.price_history = {}
        self.returns = {}
        self.peak_price = {}
    
    def process_element(self, value, ctx):
        data = json.loads(value) if isinstance(value, str) else value
        symbol = data['symbol']
        price = float(data['close'])
        
        if symbol not in self.price_history:
            self.price_history[symbol] = deque(maxlen=100)
            self.returns[symbol] = deque(maxlen=100)
            self.peak_price[symbol] = price
        
        self.price_history[symbol].append(price)
        
        # Calculate returns
        if len(self.price_history[symbol]) > 1:
            ret = (price - list(self.price_history[symbol])[-2]) / list(self.price_history[symbol])[-2]
            self.returns[symbol].append(ret)
        
        # Calculate drawdown
        self.peak_price[symbol] = max(self.peak_price[symbol], price)
        drawdown = (self.peak_price[symbol] - price) / self.peak_price[symbol]
        
        # Calculate volatility and Sharpe ratio
        volatility = 0
        sharpe_ratio = 0
        var_95 = 0
        
        if len(self.returns[symbol]) >= 20:
            returns_list = list(self.returns[symbol])
            mean_return = sum(returns_list) / len(returns_list)
            variance = sum((r - mean_return) ** 2 for r in returns_list) / len(returns_list)
            volatility = variance ** 0.5
            
            if volatility > 0:
                sharpe_ratio = (mean_return * 252) / (volatility * (252 ** 0.5))  # Annualized
            
            # Value at Risk (95%)
            sorted_returns = sorted(returns_list)
            var_index = int(len(sorted_returns) * 0.05)
            var_95 = sorted_returns[var_index] if var_index < len(sorted_returns) else sorted_returns[0]
        
        result = {
            'symbol': symbol,
            'price': price,
            'drawdown_pct': round(drawdown * 100, 2),
            'volatility': round(volatility * 100, 2),
            'sharpe_ratio': round(sharpe_ratio, 2),
            'var_95_pct': round(var_95 * 100, 2),
            'type': 'risk_metrics'
        }
        
        yield json.dumps(result)


# ============================================================================
# MAIN HANDLER CLASS
# ============================================================================
class FlinkStockHandler:
    def __init__(self, topic_in, topic_out, kafka_servers):
        self.topic_in = topic_in
        self.topic_out = topic_out
        self.kafka_servers = kafka_servers
        self.env = StreamExecutionEnvironment.get_execution_environment()
        config = Configuration()
        

        config.set_string("jobmanager.rpc.address", "jobmanager")
        config.set_string("jobmanager.rpc.port", "6123")
        config.set_string("rest.port", "8081")
        config.set_string("pipeline.jars","file:///opt/flink/lib/flink-connector-kafka-4.0.0-2.0.jar")

        config.set_string("python.fn-execution.bundle.size", "1000")
        self.env = StreamExecutionEnvironment.get_execution_environment(config)

        self.env.set_parallelism(1)
        
        self.env.enable_checkpointing(5000)

        self.source = (
            KafkaSource.builder()
            .set_bootstrap_servers(self.kafka_servers)
            .set_topics(self.topic_in)
            .set_group_id("stock_group")
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )
        
        self.watermark_strategy = (
            WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(10))
            .with_timestamp_assigner(StockTimestampAssigner())
        )
        
        # Kafka Sink
        self.sink = (
            KafkaSink.builder()
            .set_bootstrap_servers(self.kafka_servers)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(self.topic_out)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )
    
    def process_all_use_cases(self):
        """Run all 6 use cases optimized for REALTIME streaming"""
        logger.info("Starting REALTIME stock processing pipeline...")
        
        # Base stream
        base_stream = self.env.from_source(
            self.source,
            self.watermark_strategy,
            "Kafka Source"
        )
        
        # USE CASE 1: OHLC Realtime (1min, 5min, 15min windows)
        ohlc_stream = (
            base_stream
            .key_by(lambda x: json.loads(x)['symbol'])
            .process(OHLCRealtimeProcessor())
        )
        logger.info("✅ OHLC Realtime processing set up.")

        # Union all streams and sink to Kafka
        
        # Print to console for debugging
        ohlc_stream.print()
        
        # Sink to Kafka
        ohlc_stream.sink_to(self.sink)
        
        logger.info("✅ Executing REALTIME stock processing pipeline...")
        self.env.execute("Realtime Stock Analysis Pipeline")
        logger.info("✅ REALTIME stock processing pipeline execution finished.")

if __name__ == "__main__":
    handler = FlinkStockHandler(
        topic_in='stock_data',
        topic_out='stock_processed',
        kafka_servers='kafka_broker:19092'
    )
    handler.process_all_use_cases()
    logger.info("Flink Stock Handler execution completed.")