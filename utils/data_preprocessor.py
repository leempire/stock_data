import pandas as pd
import numpy as np
from typing import Optional
from joblib import delayed, Parallel
import pandas as pd


def by_time_add_rank_label(day, data, target_cols):
    """截面label处理

    Args:
        day (_type_): _description_
        data (_type_): _description_

    Returns:
        _type_: _description_
    """

    fea = []
    for col in target_cols:
        data.loc[:, f'{col}_std'] = data[col]
        data.loc[:, f'{col}_rank_std'] = data[col].rank(method='min')
        fea.append(f'{col}_std')
        fea.append(f'{col}_rank_std')
    mean = data[fea].mean()
    std = data[fea].std()
    # 这个归一化的思路是按照全市场去归一化的，后续可以尝试单个股票进行归一化；
    data[fea] = data[fea].apply(lambda x: (x - mean[x.name]) / (std[x.name] + 1e-6))
    
    return data[['code', 'day'] + fea]


def parallel_by_time_add_label(data, target_cols, num_workers=None):
    """
        parallel Cross Section Labeling
    """

    jobs = num_workers or -1
    results = Parallel(n_jobs=jobs, verbose=1)(
        delayed(by_time_add_rank_label)(time, new_group, target_cols)
        for time, new_group in data.groupby('day')
    )
    df_combined = pd.concat(results, ignore_index=True)
    return df_combined


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
        reg_data = parallel_by_time_add_label(df, ['Ret_t11'])
        df = pd.merge(df, reg_data, on=['code', 'day'], how='left')
        
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
    
    @staticmethod
    def preprocess_daily_data_v2(df: pd.DataFrame) -> pd.DataFrame:
        """预处理日线数据(v2版本)，使用特定特征顺序，对齐QuantModel的ft1
        
        Args:
            df: 经过preprocess_daily_data_basic处理后的DataFrame
            
        Returns:
            预处理后的DataFrame，包含特定顺序的特征
        """
        print('正在进行v2版本特征工程...')
        # 确保所有需要的列都存在
        required_cols = ['vol', 'close', 'open', 'low', 'high', 'amount']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"缺少必要列: {col}")
        
        # 计算adjvwap (成交量加权平均价格)
        df['adjvwap'] = df['amount'] / (df['vol'] + 1e-6)
        
        # 按指定顺序选择特征
        cols = ['code', 'day', 'open', 'high', 'low', 'close', 'vol', 'amount', 'adjvwap', 'SecurityID', 'time', 'Ret_t11', 'Ret_t11_std', 'Ret_t11_rank_std']
        df = df[cols].copy()
        
        print('v2版本特征工程完成')
        return df
    
    @staticmethod
    def preprocess_daily_data_v3(df: pd.DataFrame, fit_start: int, fit_end: int) -> pd.DataFrame:
        """预处理日线数据(v3版本)，按指定日期范围进行标准化
        
        Args:
            df: 经过preprocess_daily_data_basic处理后的DataFrame
            fit_start: 标准化开始日期(格式: YYYYMMDD)
            fit_end: 标准化结束日期(格式: YYYYMMDD)
            
        Returns:
            标准化后的DataFrame
        """
        print('正在进行v3版本特征工程...')
        # 确保day列是整数类型
        df['day'] = df['day'].astype(int)
        
        # 获取标准化区间数据
        fit_mask = (df['day'] >= fit_start) & (df['day'] <= fit_end)
        fit_df = df[fit_mask].copy()
        
        # 不需要标准化的列
        exclude_cols = ['code', 'day', 'SecurityID', 'time', 'Ret_t11', 'Ret_t11_std', 'Ret_t11_rank_std']
        numeric_cols = [col for col in df.columns if col not in exclude_cols and df[col].dtype in ['float64', 'int64']]
        
        # 计算Robust标准化参数(中位数和四分位距)
        medians = fit_df[numeric_cols].median()
        q1 = fit_df[numeric_cols].quantile(0.25)
        q3 = fit_df[numeric_cols].quantile(0.75)
        iqrs = q3 - q1
        
        # 对所有数据进行Robust标准化
        for col in numeric_cols:
            df[col] = (df[col] - medians[col]) / (iqrs[col] + 1e-6)
        
        print('v3版本特征工程完成')
        return df