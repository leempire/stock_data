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
                print(f"\r已跳过 {code}...", end='   ')
                continue
                
            try:
                print(f"\r正在下载 {code}...", end=' ')
                df = self.tushare_api.get_single_stock_daily(
                    code=code,
                    start_date=start_date,
                    end_date=end_date
                )
                
                # 保存到数据库
                self._save_to_db(df, code)
                
            except Exception as e:
                print(f"\r下载 {code} 失败: {e}")
                continue

    def _check_code_exists(self, code: str) -> bool:
        """检查股票代码是否已存在于数据库"""
        sql = "SELECT 1 FROM stock_daily WHERE ts_code = %s LIMIT 1"
        result = self.db.query(sql, (code,))
        return len(result) > 0

    def _save_to_db(self, df: pd.DataFrame, code: str):
        """保存数据到MySQL数据库"""
        # 准备批量插入数据，将NaN替换为None
        data = [(
            row['ts_code'],
            row['trade_date'],
            None if pd.isna(row['open']) else row['open'],
            None if pd.isna(row['high']) else row['high'],
            None if pd.isna(row['low']) else row['low'],
            None if pd.isna(row['close']) else row['close'],
            None if pd.isna(row['pre_close']) else row['pre_close'],
            None if pd.isna(row['change']) else row['change'],
            None if pd.isna(row['pct_chg']) else row['pct_chg'],
            None if pd.isna(row['vol']) else row['vol'],
            None if pd.isna(row['amount']) else row['amount']
        ) for _, row in df.iterrows()]
        
        sql = """
        INSERT INTO stock_daily 
        (ts_code, trade_date, open, high, low, close, pre_close, 
         `change`, pct_chg, vol, amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open=VALUES(open), high=VALUES(high), low=VALUES(low),
            close=VALUES(close), pre_close=VALUES(pre_close),
            `change`=VALUES(`change`), pct_chg=VALUES(pct_chg),
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

    def export_daily_to_parquet(self, output_path: str, batch_size: int = 10_0000, code_prefixes: List[str] = None):
        """从数据库高效导出stock_daily数据为parquet文件
        
        Args:
            output_path: 输出文件路径
            batch_size: 分批查询的每批数据量，默认10万条
            code_prefixes: 要导出的股票代码前缀列表，如['600','601','000']
        """
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        print("正在从数据库分批导出股票日线数据...")
        
        # 1. 构建WHERE条件和参数
        where_clause = ""
        params = ()
        if code_prefixes:
            conditions = " OR ".join(["ts_code LIKE %s" for _ in code_prefixes])
            where_clause = f"WHERE {conditions}"
            params = tuple(f"{prefix}%" for prefix in code_prefixes)
        
        # 2. 先获取总行数
        count_sql = f"SELECT COUNT(*) as total FROM stock_daily {where_clause}"
        total = self.db.query(count_sql, params)[0]['total']
        print(f"共找到{total}条符合条件的股票日线数据")
        
        if total == 0:
            print("数据库中没有符合条件的股票日线数据")
            return
            
        # 3. 分批查询
        dfs = []
        for offset in range(0, total, batch_size):
            print(f"正在获取第 {offset+1}-{min(offset+batch_size, total)} 条数据...")
            sql = f"""
            SELECT * FROM stock_daily 
            {where_clause}
            LIMIT {batch_size} OFFSET {offset}
            """
            batch = pd.DataFrame(self.db.query(sql, params))
            batch['trade_date'] = pd.to_datetime(batch['trade_date'], format='%Y%m%d')
            dfs.append(batch)
            
        # 4. 合并并保存
        df = pd.concat(dfs, ignore_index=True)
        df.to_parquet(output_path, index=False)
        print(f"成功导出{len(df)}条数据到 {output_path} (共{total}条)")
        return df