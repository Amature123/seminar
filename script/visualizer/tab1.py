import pandas as pd 
from bokeh.plotting import show
from bokeh.models import ColumnDataSource, DataTable, TableColumn,LinearAxis, Range1d, HoverTool, DatetimeTickFormatter, NumeralTickFormatter, Span
from bokeh.sampledata.stocks import AAPL
from processing.trading_handler import OHVLC
source = ColumnDataSource(data=dict(
    symbol=[],
    price=[],
    change=[],
    volume=[],
    bid=[],
    ask=[],
    time=[]
))

def compare_plot():
  stats = PreText(text='', width=500)
  stats.text = str(source.data)
  def 