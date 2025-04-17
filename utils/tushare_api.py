import os
import tushare as ts
import pandas as pd
from datetime import datetime
import json

class TushareAPI:
    """
    Tushare API封装类，用于获取和处理股票数据
    
    功能包括：
    - 获取股票列表信息
    - 按日期获取股票交易数据
    - 获取单只股票的历史日线数据
    
    属性:
        pro: tushare pro_api实例
        data_path: 数据存储路径
        
    方法:
        get_stock_codes(): 获取所有上市股票代码列表
        get_stock_by_date(date): 获取指定日期的所有股票交易数据
        get_single_stock_daily(code, start_date, end_date): 获取单只股票的历史日线数据
        
    示例:
        >>> api = TushareAPI(data_path='./data', token='your_tushare_token')
        >>> df = api.get_stock_codes()
    """
    
    def __init__(self, data_path, token):
        self.pro = ts.pro_api(token)
        self.data_path = data_path
    
    def get_stock_codes(self, save=True):
        """该api每小时只能调用一次，建议保存到本地文件，避免重复调用"""
        stock_list = self.pro.stock_basic(exchange='', list_status='L')
        if save:
            os.makedirs(self.data_path, exist_ok=True)  # 新增目录检查
            stock_list.to_csv(os.path.join(self.data_path, 'stock_list.csv'), index=False)
        return stock_list
        
    def get_stock_by_date(self, date):
        df = self.pro.daily(trade_date=date)
        df['trade_date'] = df['trade_date'].astype(int)
        return df
    
    def get_single_stock_daily(self, code, start_date='20160101', end_date=None):
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
            
        df = self.pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
        df = df.sort_values('trade_date')
        return df
