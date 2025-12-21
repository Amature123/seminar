import React, { useState, useEffect, useMemo } from 'react';
import { ChevronUp, ChevronDown, Search, RefreshCw, AlertCircle, BarChart3, Table } from 'lucide-react';

const TradingDashboard = () => {
  const [activeTab, setActiveTab] = useState('table');
  const [data, setData] = useState([]);
  const [ohlcData, setOhlcData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [ohlcLoading, setOhlcLoading] = useState(false);
  const [error, setError] = useState(null);
  const [ohlcError, setOhlcError] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedSymbol, setSelectedSymbol] = useState('');
  const [sortConfig, setSortConfig] = useState({ key: 'symbol', direction: 'asc' });

  // API endpoints
  const API_URL = 'http://localhost:8000/trading/summary';
  const OHLC_API_URL = 'http://localhost:8000/ohlvc/summary';

  const fetchData = async (symbols = null) => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (symbols && symbols.length > 0) {
        params.append('symbols', symbols.join(','));
      }
      const url = `${API_URL}?${params.toString()}`;
      console.log('Fetching from:', url);
      const response = await fetch(url);
      console.log('Response status:', response.status);
      if (!response.ok) {
        throw new Error(`API Error: ${response.statusText}`);
      }
      const result = await response.json();
      console.log('Data received:', result);
      setData(result.data || result);
    } catch (err) {
      setError(err.message);
      console.error('Fetch error:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  useEffect(() => {
    if (activeTab === 'candle' || activeTab === 'ohlc') {
      fetchOhlcData();
    }
  }, [activeTab]);

  const fetchOhlcData = async (symbol = null) => {
    setOhlcLoading(true);
    setOhlcError(null);
    try {
      const params = new URLSearchParams();
      if (symbol) {
        params.append('symbol', symbol);
      }
      const url = `${OHLC_API_URL}?${params.toString()}`;
      console.log('Fetching OHLC from:', url);
      const response = await fetch(url);
      console.log('OHLC Response status:', response.status);
      if (!response.ok) {
        throw new Error(`API Error: ${response.statusText}`);
      }
      const result = await response.json();
      console.log('OHLC Data received:', result);
      setOhlcData(result.data || result);
      
      // Set selected symbol to first symbol if not set
      if (!selectedSymbol && result.data && result.data.length > 0) {
        setSelectedSymbol(result.data[0].symbol);
      }
    } catch (err) {
      setOhlcError(err.message);
      console.error('OHLC Fetch error:', err);
    } finally {
      setOhlcLoading(false);
    }
  };

  const columns = [
    { key: 'index', label: 'STT', width: 60, sticky: false, isIndex: true },
    { key: 'symbol', label: 'Mã', width: 70, sticky: true },
    { key: 'handle_time', label: 'Thời gian', width: 160 },
    { key: 'reference', label: 'Tham chiếu', width: 90 },
    { key: 'ceiling', label: 'Trần', width: 80 },
    { key: 'floor', label: 'Sàn', width: 80 },
    { key: 'room_foreign', label: 'Room NN', width: 100 },
    { key: 'foreign_buy_volume', label: 'NN Mua', width: 100 },
    { key: 'foreign_sell_volume', label: 'NN Bán', width: 100 },
    { key: 'buy_1', label: 'Mua 1', width: 70 },
    { key: 'buy_1_volume', label: 'KL Mua 1', width: 100 },
    { key: 'buy_2', label: 'Mua 2', width: 70 },
    { key: 'buy_2_volume', label: 'KL Mua 2', width: 100 },
    { key: 'buy_3', label: 'Mua 3', width: 70 },
    { key: 'buy_3_volume', label: 'KL Mua 3', width: 100 },
    { key: 'sell_1', label: 'Bán 1', width: 70 },
    { key: 'sell_1_volume', label: 'KL Bán 1', width: 100 },
    { key: 'sell_2', label: 'Bán 2', width: 70 },
    { key: 'sell_2_volume', label: 'KL Bán 2', width: 100 },
    { key: 'sell_3', label: 'Bán 3', width: 70 },
    { key: 'sell_3_volume', label: 'KL Bán 3', width: 100 },
    { key: 'highest', label: 'Cao nhất', width: 80 },
    { key: 'lowest', label: 'Thấp nhất', width: 80 },
    { key: 'average', label: 'Trung bình', width: 80 },
  ];

  const filteredData = useMemo(() => {
    return data.filter(row => 
      row.symbol?.toLowerCase().includes(searchTerm.toLowerCase())
    );
  }, [data, searchTerm]);

  const sortedData = useMemo(() => {
    const sorted = [...filteredData];
    if (sortConfig.key) {
      sorted.sort((a, b) => {
        let aVal = a[sortConfig.key];
        let bVal = b[sortConfig.key];
        
        if (sortConfig.key === 'handle_time') {
          aVal = new Date(aVal).getTime();
          bVal = new Date(bVal).getTime();
        } else if (typeof aVal === 'string') {
          return sortConfig.direction === 'asc' 
            ? aVal.localeCompare(bVal)
            : bVal.localeCompare(aVal);
        }
        
        return sortConfig.direction === 'asc'
          ? aVal - bVal
          : bVal - aVal;
      });
    }
    return sorted;
  }, [filteredData, sortConfig]);

  const handleSort = (key) => {
    setSortConfig(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'asc' ? 'desc' : 'asc'
    }));
  };

  const formatNumber = (num) => {
    if (num === null || num === undefined) return '-';
    if (typeof num === 'string') return num;
    return Number(num).toLocaleString('vi-VN', {
      minimumFractionDigits: 0,
      maximumFractionDigits: 2
    });
  };

  const formatDateTime = (dateStr) => {
    if (!dateStr) return '-';
    try {
      return new Date(dateStr).toLocaleString('vi-VN');
    } catch {
      return dateStr;
    }
  };

  const SortIcon = ({ column }) => {
    if (sortConfig.key !== column.key) return <div className="w-4 h-4" />;
    return sortConfig.direction === 'asc' 
      ? <ChevronUp size={16} className="text-blue-500" />
      : <ChevronDown size={16} className="text-blue-500" />;
  };

  const uniqueSymbols = [...new Set(data.map(d => d.symbol))];
  const uniqueOhlcSymbols = [...new Set(ohlcData.map(d => d.symbol))];

  // Filter OHLC data by selected symbol
  const filteredOhlcData = selectedSymbol 
    ? ohlcData.filter(d => d.symbol === selectedSymbol).sort((a, b) => new Date(a.time) - new Date(b.time))
    : [];

  const tabs = [
    { id: 'table', label: 'Bảng Giao Dịch', icon: Table },
    { id: 'ohlc', label: 'Biểu Đồ OHLC', icon: BarChart3 },
    { id: 'candle', label: 'Biểu Đồ Nến', icon: BarChart3 },
  ];

  return (
    <div className="w-full bg-gray-100 min-h-screen">
      {/* Header với Title và Tabs */}
      <div className="bg-white shadow-md">
        <div className="max-w-full p-4">
          <h1 className="text-3xl font-bold text-gray-800 mb-4">TRADING DASHBOARD</h1>
          
          {/* Tabs */}
          <div className="flex gap-2 border-b-2 border-gray-300">
            {tabs.map(tab => {
              const Icon = tab.icon;
              return (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`flex items-center gap-2 px-6 py-3 font-semibold transition-all ${
                    activeTab === tab.id
                      ? 'bg-blue-600 text-white border-b-4 border-blue-800'
                      : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                  }`}
                  style={{
                    border: '2px solid #374151',
                    borderBottom: activeTab === tab.id ? '4px solid #1e40af' : '2px solid #374151'
                  }}
                >
                  <Icon size={20} />
                  {tab.label}
                </button>
              );
            })}
          </div>
        </div>
      </div>

      {/* Search Bar */}
      <div className="max-w-full p-4">
        <div className="flex items-center gap-2 bg-white p-3 rounded-lg shadow-md" style={{ border: '2px solid #374151' }}>
          <Search size={20} className="text-gray-400" />
          <input
            type="text"
            placeholder="TÌM KIẾM MÃ CHỨNG KHOÁN..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="flex-1 outline-none text-gray-700 font-semibold text-lg"
          />
          <button
            onClick={() => fetchData()}
            disabled={loading}
            className="p-2 hover:bg-gray-100 rounded-lg transition-colors disabled:opacity-50"
            title="Làm mới dữ liệu"
          >
            <RefreshCw size={20} className={loading ? 'animate-spin' : 'text-gray-600'} />
          </button>
        </div>
      </div>

      {/* Content Area */}
      <div className="max-w-full p-4">
        <div className="bg-white rounded-lg shadow-lg p-6" style={{ border: '3px solid #1f2937', minHeight: '500px' }}>
          {activeTab === 'table' && (
            <div>
              {/* Error Message */}
              {error && (
                <div className="mb-4 p-4 bg-red-50 border border-red-200 rounded-lg flex items-start gap-3">
                  <AlertCircle size={20} className="text-red-600 mt-0.5 flex-shrink-0" />
                  <div>
                    <p className="font-semibold text-red-800">Lỗi kết nối</p>
                    <p className="text-red-700 text-sm">{error}</p>
                    <p className="text-red-600 text-xs mt-2">Đảm bảo backend API đang chạy tại: {API_URL}</p>
                  </div>
                </div>
              )}

              {/* Loading State */}
              {loading && (
                <div className="text-center py-8">
                  <div className="inline-flex items-center gap-2 text-blue-600">
                    <RefreshCw size={20} className="animate-spin" />
                    <span>Đang tải dữ liệu...</span>
                  </div>
                </div>
              )}

              {/* Table */}
              {!loading && (
                <div 
                  className="rounded-lg overflow-hidden shadow-lg"
                  style={{ border: '3px solid #1f2937' }}
                >
                  <div className="overflow-x-auto">
                    <table className="w-full" style={{ borderCollapse: 'collapse' }}>
                      <thead>
                        <tr className="text-white text-sm font-semibold sticky top-0" style={{ background: '#6e9bdaff' }}>
                          {columns.map(col => (
                            <th
                              key={col.key}
                              style={{ 
                                width: col.width,
                                border: '2px solid #374151',
                                padding: '12px'
                              }}
                              className={`text-center cursor-pointer hover:bg-blue-900 transition-colors whitespace-nowrap ${
                                col.sticky ? 'sticky left-0 z-10' : ''
                              }`}
                              onClick={() => handleSort(col.key)}
                            >
                              <div className="flex items-center justify-center gap-1">
                                <span>{col.label}</span>
                                <SortIcon column={col} />
                              </div>
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {sortedData.map((row, idx) => (
                          <tr
                            key={`${row.symbol}-${row.handle_time}-${idx}`}
                            className={`hover:bg-blue-50 transition-colors ${
                              idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'
                            }`}
                          >
                            {columns.map(col => {
                              let backgroundColor = '';
                              if (col.key === 'index') {
                                backgroundColor = '#ac7510ff';
                              } else if (col.key === 'symbol') {
                                backgroundColor = '#375379ff';
                              } else if (col.key === 'handle_time') {
                                backgroundColor = '#387786ff';
                              } else if (col.key === 'reference') {
                                backgroundColor = '#758638ff';
                              } else if (col.key === 'ceiling') {
                                backgroundColor = '#758638ff';
                              } else if (col.key === 'floor') {
                                backgroundColor = '#758638ff';
                              } else if (col.key === 'highest') {
                                backgroundColor = '#387786ff';
                              } else if (col.key === 'lowest') {
                                backgroundColor = '#387786ff';
                              } else if (col.key === 'average') {
                                backgroundColor = '#387786ff';
                              } else if (col.key === 'room_foreign') {
                                backgroundColor = '#387786ff';
                              } else if (col.key === 'foreign_buy_volume') {
                                backgroundColor = '#2a5f3dff';
                              } else if (col.key === 'foreign_sell_volume') {
                                backgroundColor = '#b14242ff';
                              } else if (col.key === 'buy_1') {
                                backgroundColor = '#2a5f3dff';
                              } else if (col.key === 'buy_1_volume') {
                                backgroundColor = '#2a5f3dff';
                              } else if (col.key === 'buy_2') {
                                backgroundColor = '#2a5f3dff';
                              } else if (col.key === 'buy_2_volume') {
                                backgroundColor = '#2a5f3dff';
                              } else if (col.key === 'buy_3') {
                                backgroundColor = '#2a5f3dff';
                              } else if (col.key === 'buy_3_volume') {
                                backgroundColor = '#2a5f3dff';
                              } else if (col.key === 'sell_1') {
                                backgroundColor = '#b14242ff';
                              } else if (col.key === 'sell_1_volume') {
                                backgroundColor = '#b14242ff';
                              } else if (col.key === 'sell_2') {
                                backgroundColor = '#b14242ff';
                              } else if (col.key === 'sell_2_volume') {
                                backgroundColor = '#b14242ff';
                              } else if (col.key === 'sell_3') {
                                backgroundColor = '#b14242ff';
                              } else if (col.key === 'sell_3_volume') {
                                backgroundColor = '#b14242ff';
                              }
                              
                              return (
                                <td
                                  key={col.key}
                                  style={{ 
                                    width: col.width,
                                    border: '1px solid #9ca3af',
                                    padding: '10px',
                                    fontWeight: 'bold',
                                    backgroundColor: backgroundColor || 'inherit'
                                  }}
                                  className={`text-center text-sm ${
                                    col.sticky ? 'sticky left-0 z-9 text-blue-600' : ''
                                  }`}
                                >
                                  {col.isIndex
                                    ? idx + 1
                                    : col.key === 'handle_time'
                                    ? formatDateTime(row[col.key])
                                    : formatNumber(row[col.key])
                                  }
                                </td>
                              );
                            })}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  {sortedData.length === 0 && !loading && (
                    <div className="text-center py-8 text-gray-500">
                      Không tìm thấy dữ liệu
                    </div>
                  )}
                </div>
              )}

              {/* Stats */}
              {!loading && sortedData.length > 0 && (
                <div className="mt-4 text-sm text-gray-600">
                  Hiển thị <span className="font-semibold">{sortedData.length}</span> trên <span className="font-semibold">{data.length}</span> dòng
                </div>
              )}
            </div>
          )}

          {activeTab === 'ohlc' && (
            <div className="flex items-center justify-center h-full">
              <div className="text-center">
                <BarChart3 size={64} className="mx-auto mb-4 text-gray-400" />
                <h2 className="text-2xl font-bold text-gray-800 mb-2">Biểu Đồ OHLC</h2>
                <p className="text-gray-600">Nội dung biểu đồ OHLC sẽ được hiển thị ở đây</p>
              </div>
            </div>
          )}

          {activeTab === 'candle' && (
            <div>
              {/* Symbol Selector */}
              <div className="mb-4">
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Chọn mã chứng khoán:
                </label>
                <select
                  value={selectedSymbol}
                  onChange={(e) => setSelectedSymbol(e.target.value)}
                  className="px-4 py-2 border-2 border-gray-300 rounded-lg outline-none focus:border-blue-500 font-semibold"
                >
                  {uniqueOhlcSymbols.map(sym => (
                    <option key={sym} value={sym}>{sym}</option>
                  ))}
                </select>
                <button
                  onClick={() => fetchOhlcData(selectedSymbol)}
                  disabled={ohlcLoading}
                  className="ml-3 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50"
                >
                  <RefreshCw size={16} className={ohlcLoading ? 'animate-spin inline mr-2' : 'inline mr-2'} />
                  Làm mới
                </button>
              </div>

              {/* Error Message */}
              {ohlcError && (
                <div className="mb-4 p-4 bg-red-50 border border-red-200 rounded-lg flex items-start gap-3">
                  <AlertCircle size={20} className="text-red-600 mt-0.5 flex-shrink-0" />
                  <div>
                    <p className="font-semibold text-red-800">Lỗi kết nối</p>
                    <p className="text-red-700 text-sm">{ohlcError}</p>
                    <p className="text-red-600 text-xs mt-2">Đảm bảo backend API đang chạy tại: {OHLC_API_URL}</p>
                  </div>
                </div>
              )}

              {/* Loading State */}
              {ohlcLoading && (
                <div className="text-center py-8">
                  <div className="inline-flex items-center gap-2 text-blue-600">
                    <RefreshCw size={20} className="animate-spin" />
                    <span>Đang tải dữ liệu OHLC...</span>
                  </div>
                </div>
              )}

              {/* Candlestick Chart */}
              {!ohlcLoading && filteredOhlcData.length > 0 && (
                <div className="bg-white rounded-lg p-4" style={{ border: '2px solid #374151' }}>
                  <h3 className="text-xl font-bold text-gray-800 mb-4 text-center">
                    Biểu Đồ Nến - {selectedSymbol}
                  </h3>
                  <svg width="100%" height="600" className="overflow-visible">
                    {/* Chart will be rendered here */}
                    {(() => {
                      const width = 1200;
                      const height = 600;
                      const margin = { top: 20, right: 50, bottom: 60, left: 60 };
                      const chartWidth = width - margin.left - margin.right;
                      const chartHeight = height - margin.top - margin.bottom;

                      // Calculate scales
                      const dataLength = filteredOhlcData.length;
                      const candleWidth = Math.max(2, Math.min(20, chartWidth / dataLength * 0.7));
                      const candleSpacing = chartWidth / dataLength;

                      const allPrices = filteredOhlcData.flatMap(d => [d.high, d.low]);
                      const minPrice = Math.min(...allPrices) * 0.99;
                      const maxPrice = Math.max(...allPrices) * 1.01;
                      const priceRange = maxPrice - minPrice;

                      const priceScale = (price) => {
                        return chartHeight - ((price - minPrice) / priceRange) * chartHeight;
                      };

                      return (
                        <g transform={`translate(${margin.left}, ${margin.top})`}>
                          {/* Grid lines */}
                          {[0, 0.25, 0.5, 0.75, 1].map((ratio, i) => {
                            const y = chartHeight * ratio;
                            const price = maxPrice - (priceRange * ratio);
                            return (
                              <g key={i}>
                                <line
                                  x1={0}
                                  y1={y}
                                  x2={chartWidth}
                                  y2={y}
                                  stroke="#e5e7eb"
                                  strokeWidth={1}
                                />
                                <text
                                  x={-10}
                                  y={y + 4}
                                  textAnchor="end"
                                  fontSize={12}
                                  fill="#6b7280"
                                >
                                  {price.toFixed(2)}
                                </text>
                              </g>
                            );
                          })}

                          {/* Candlesticks */}
                          {filteredOhlcData.map((d, i) => {
                            const x = i * candleSpacing + candleSpacing / 2;
                            const isGreen = d.close >= d.open;
                            const color = isGreen ? '#16a34a' : '#dc2626';
                            const bodyTop = priceScale(Math.max(d.open, d.close));
                            const bodyBottom = priceScale(Math.min(d.open, d.close));
                            const bodyHeight = Math.max(1, bodyBottom - bodyTop);

                            return (
                              <g key={i}>
                                {/* High-Low line */}
                                <line
                                  x1={x}
                                  y1={priceScale(d.high)}
                                  x2={x}
                                  y2={priceScale(d.low)}
                                  stroke={color}
                                  strokeWidth={1}
                                />
                                {/* Body */}
                                <rect
                                  x={x - candleWidth / 2}
                                  y={bodyTop}
                                  width={candleWidth}
                                  height={bodyHeight}
                                  fill={color}
                                  stroke={color}
                                  strokeWidth={1}
                                />
                                {/* Time label (every 10th candle) */}
                                {i % Math.max(1, Math.floor(dataLength / 10)) === 0 && (
                                  <text
                                    x={x}
                                    y={chartHeight + 20}
                                    textAnchor="middle"
                                    fontSize={10}
                                    fill="#6b7280"
                                    transform={`rotate(-45, ${x}, ${chartHeight + 20})`}
                                  >
                                    {new Date(d.time).toLocaleString('vi-VN', { 
                                      month: 'short', 
                                      day: 'numeric',
                                      hour: '2-digit',
                                      minute: '2-digit'
                                    })}
                                  </text>
                                )}
                              </g>
                            );
                          })}

                          {/* Axes */}
                          <line
                            x1={0}
                            y1={chartHeight}
                            x2={chartWidth}
                            y2={chartHeight}
                            stroke="#374151"
                            strokeWidth={2}
                          />
                          <line
                            x1={0}
                            y1={0}
                            x2={0}
                            y2={chartHeight}
                            stroke="#374151"
                            strokeWidth={2}
                          />

                          {/* Labels */}
                          <text
                            x={chartWidth / 2}
                            y={chartHeight + 50}
                            textAnchor="middle"
                            fontSize={14}
                            fontWeight="bold"
                            fill="#374151"
                          >
                            Thời gian
                          </text>
                          <text
                            x={-chartHeight / 2}
                            y={-40}
                            textAnchor="middle"
                            fontSize={14}
                            fontWeight="bold"
                            fill="#374151"
                            transform={`rotate(-90, -${chartHeight / 2}, -40)`}
                          >
                            Giá
                          </text>
                        </g>
                      );
                    })()}
                  </svg>

                  {/* Stats */}
                  <div className="mt-4 grid grid-cols-2 md:grid-cols-5 gap-4">
                    {filteredOhlcData.length > 0 && (() => {
                      const latest = filteredOhlcData[filteredOhlcData.length - 1];
                      return (
                        <>
                          <div className="bg-gray-50 p-3 rounded border">
                            <p className="text-xs text-gray-600">Mở cửa</p>
                            <p className="text-lg font-bold">{latest.open.toFixed(2)}</p>
                          </div>
                          <div className="bg-gray-50 p-3 rounded border">
                            <p className="text-xs text-gray-600">Cao nhất</p>
                            <p className="text-lg font-bold text-green-600">{latest.high.toFixed(2)}</p>
                          </div>
                          <div className="bg-gray-50 p-3 rounded border">
                            <p className="text-xs text-gray-600">Thấp nhất</p>
                            <p className="text-lg font-bold text-red-600">{latest.low.toFixed(2)}</p>
                          </div>
                          <div className="bg-gray-50 p-3 rounded border">
                            <p className="text-xs text-gray-600">Đóng cửa</p>
                            <p className="text-lg font-bold">{latest.close.toFixed(2)}</p>
                          </div>
                          <div className="bg-gray-50 p-3 rounded border">
                            <p className="text-xs text-gray-600">Khối lượng</p>
                            <p className="text-lg font-bold">{latest.volume.toLocaleString('vi-VN')}</p>
                          </div>
                        </>
                      );
                    })()}
                  </div>
                </div>
              )}

              {!ohlcLoading && filteredOhlcData.length === 0 && (
                <div className="text-center py-8 text-gray-500">
                  Không có dữ liệu OHLC cho mã {selectedSymbol}
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default TradingDashboard;