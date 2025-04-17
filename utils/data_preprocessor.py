import pandas as pd
import numpy as np
from typing import Optional

class DataPreprocessor:
    """股票数据预处理工具类"""
    
    @staticmethod
    def preprocess_daily_data_basic(df: pd.DataFrame) -> pd.DataFrame:
        """预处理日线数据
        
        Args:
            df: 原始数据DataFrame，需包含stock_daily表的所有字段
            
        Returns:
            预处理后的DataFrame
        """
        print('正在进行基础预处理...')
        # 复制数据避免修改原始数据
        df = df.copy()
        
        # 转换Decimal类型为float
        numeric_cols = ['open', 'high', 'low', 'close', 'pre_close', 
                       'change', 'pct_chg', 'vol', 'amount']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].astype(float)
        
        # 1. 重命名列
        df = df.rename(columns={
            'trade_date': 'day',
            'ts_code': 'code'
        })
        
        # 2. 转换股票代码格式 (000001.SZ -> 1)
        df['code'] = df['code'].str.split('.').str[0].astype(int)
        
        # 3. 添加SecurityID列
        df['SecurityID'] = df['code']
        
        # 4. 添加time列
        df['day'] = df['day'].dt.strftime('%Y%m%d').astype(int)
        df['time'] = df['day']
        
        # 5. 计算Ret_t11 (11天后开盘价/1天后开盘价 - 1)
        df = df.sort_values(['code', 'day'])
        df['open_1'] = df.groupby('code')['open'].shift(-1)
        df['open_11'] = df.groupby('code')['open'].shift(-11)
        df['Ret_t11'] = df['open_11'] / df['open_1'] - 1
        
        # 6. 计算Ret_t11的std和rank_std
        target_cols = ['Ret_t11']
        fea = []
        for col in target_cols:
            df.loc[:, f'{col}_std'] = df[col]
            df.loc[:, f'{col}_rank_std'] = df[col].rank(method='min')
            fea.append(f'{col}_std')
            fea.append(f'{col}_rank_std')
        
        # 归一化处理
        mean = df[fea].mean()
        std = df[fea].std()
        df[fea] = df[fea].apply(lambda x: (x - mean[x.name]) / (std[x.name] + 1e-6))
        
        # 删除临时列
        df = df.drop(columns=['open_1', 'open_11'])
        print('基础预处理完成')
        return df
    
    @staticmethod
    def preprocess_daily_data_v1(df: pd.DataFrame) -> pd.DataFrame:
        """预处理日线数据(v1版本)
        
        Args:
            df: 经过preprocess_daily_data_basic处理后的DataFrame
            
        Returns:
            预处理后的DataFrame
        """
        print('正在进行v1版本特征工程...')
        # 技术指标特征
        df['price_diff'] = df['high'] - df['low']  # 当日价格波动幅度
        df['close_open_ratio'] = df['close'] / df['open']  # 收盘开盘价比
        df['volatility'] = (df['high'] - df['low']) / df['pre_close']  # 相对波动率
        
        # 量价关系特征
        df['amount_per_vol'] = df['amount'] / (df['vol'] + 1e-6)  # 单位成交量金额
        df['vol_ma5'] = df['vol'].rolling(5, min_periods=1).mean()  # 成交量5日均线
        df['vol_ma10'] = df['vol'].rolling(10, min_periods=1).mean()  # 成交量10日均线
        
        # 时间序列特征(按股票分组计算)
        df = df.sort_values(['code', 'day'])
        df['close_ma5'] = df.groupby('code')['close'].rolling(5, min_periods=1).mean().values  # 5日均线
        df['close_ma10'] = df.groupby('code')['close'].rolling(10, min_periods=1).mean().values  # 10日均线
        df['momentum'] = df['close'] / df.groupby('code')['close'].shift(5) - 1  # 5日动量
        df['momentum'] = df['momentum'].fillna(0)  # 填充缺失值
        print('v1版本特征工程完成')
        return df