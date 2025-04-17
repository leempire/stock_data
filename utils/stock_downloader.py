import os
import pandas as pd
from datetime import datetime
from typing import List, Optional
from utils.mysql_manager import MySQLManager

class StockDownloader:
    """股票数据下载器，支持断点续传
    
    功能包括：
    - 批量下载股票历史数据
    - 支持断点续传
    - 自动保存下载进度到数据库
    """
    
    def __init__(self, db: MySQLManager, tushare_api: 'TushareAPI'):
        """初始化股票下载器
        
        Args:
            db: MySQLManager实例
            tushare_api: TushareAPI实例
        """
        self.db = db
        self.tushare_api = tushare_api
        # 移除progress_file相关代码

    def download_stocks(self, 
                       stock_codes: List[str], 
                       start_date: str = '20160101', 
                       end_date: Optional[str] = None,
                       skip_existing: bool = True):
        """下载多只股票的历史数据到数据库
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，None表示今天
            skip_existing: 是否跳过已存在的股票数据，默认True
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
            
        for code in stock_codes:
            # 检查是否跳过已存在数据
            if skip_existing and self._check_code_exists(code):
                print(f"已跳过 {code} (已下载)")
                continue
                
            try:
                print(f"正在下载 {code}...")
                df = self.tushare_api.get_single_stock_daily(
                    code=code,
                    start_date=start_date,
                    end_date=end_date
                )
                
                # 保存到数据库
                self._save_to_db(df, code)
                print(f"完成下载 {code}")
                
            except Exception as e:
                print(f"下载 {code} 失败: {e}")
                continue

    def _check_code_exists(self, code: str) -> bool:
        """检查股票代码是否已存在于数据库"""
        sql = "SELECT 1 FROM stock_daily WHERE ts_code = %s LIMIT 1"
        result = self.db.query(sql, (code,))
        return len(result) > 0

    def _save_to_db(self, df: pd.DataFrame, code: str):
        """保存数据到MySQL数据库"""
        # 准备批量插入数据
        data = [(
            row['ts_code'],
            row['trade_date'],
            row['open'],
            row['high'],
            row['low'],
            row['close'],
            row['pre_close'],
            row['change'],
            row['pct_chg'],
            row['vol'],
            row['amount']
        ) for _, row in df.iterrows()]
        
        sql = """
        INSERT INTO stock_daily 
        (ts_code, trade_date, open, high, low, close, pre_close, 
         change, pct_chg, vol, amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open=VALUES(open), high=VALUES(high), low=VALUES(low),
            close=VALUES(close), pre_close=VALUES(pre_close),
            change=VALUES(change), pct_chg=VALUES(pct_chg),
            vol=VALUES(vol), amount=VALUES(amount)
        """
        self.db.executemany(sql, data)

    def update_daily_data(self, trade_date: str = None):
        """更新指定日期的所有股票数据，默认当天
        
        Args:
            trade_date: 交易日期(YYYYMMDD)，None表示当天
        """
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')
            
        print(f"正在获取{trade_date}的股票数据...")
        # 获取当日所有股票数据
        df = self.tushare_api.get_stock_by_date(trade_date)
        
        if df.empty:
            print(f"{trade_date}无交易数据")
            return
        
        # 保存到数据库
        self._save_to_db(df, '')
        print(f"成功更新{trade_date}的{len(df)}条股票数据")
    
    def update_stock_basic(self):
        """更新股票基本信息到stock_basic表"""
        print("正在获取股票基本信息...")
        df = self.tushare_api.get_stock_codes(save=False)
        
        if df.empty:
            print("获取股票信息失败")
            return
        
        # 准备批量插入数据
        data = [(
            row['ts_code'],
            row['symbol'],
            row['name'],
            row['area'],
            row['industry'],
            row['cnspell'],
            row['market'],
            row['list_date'],
            row.get('act_name', ''),
            row.get('act_ent_type', '')
        ) for _, row in df.iterrows()]
        
        sql = """
        INSERT INTO stock_basic 
        (ts_code, symbol, name, area, industry, cnspell, 
            market, list_date, act_name, act_ent_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            symbol=VALUES(symbol), name=VALUES(name),
            area=VALUES(area), industry=VALUES(industry),
            cnspell=VALUES(cnspell), market=VALUES(market),
            list_date=VALUES(list_date), act_name=VALUES(act_name),
            act_ent_type=VALUES(act_ent_type)
        """
        self.db.executemany(sql, data)
        print(f"成功更新{len(df)}条股票基本信息")