INSERT INTO print_sink
SELECT
  symbol,
  AVG(close) AS avg_close,
  MAX(high) AS max_high,
  MIN(low) AS min_low,
  window_start,
  window_end
FROM TABLE(
  TUMBLE(TABLE stock_stream, DESCRIPTOR(time), INTERVAL '1' MINUTE)
)
GROUP BY symbol, window_start, window_end;
