import os
import sys
# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import pandas as pd
from utils.initializer import ComponentInitializer
from utils.data_preprocessor import DataPreprocessor

def main():
    # 初始化组件
    init = ComponentInitializer()
    preprocessor = init.init_preprocessor()

    # 设置命令行参数
    parser = argparse.ArgumentParser(description='股票特征工程工具')
    parser.add_argument('--method', type=int, required=True,
                      choices=[1],
                      help='预处理方法: 1=v1版本特征工程')
    parser.add_argument('--input', type=str, default='./data/stock_data.parquet',
                      help='输入文件路径，默认./data/stock_data.parquet')
    parser.add_argument('--output', type=str,
                      help='输出文件路径，默认根据方法自动生成')
    args = parser.parse_args()

    # 设置默认输出路径
    if not args.output:
        args.output = f'./data/stock_data_v{args.method}.parquet'

    # 读取数据
    print(f"正在从 {args.input} 加载数据...")
    df = pd.read_parquet(args.input)

    # 根据方法选择处理方式
    if args.method == 1:
        df = preprocessor.preprocess_daily_data_v1(df)
    else:
        raise ValueError(f"未知的处理方法: {args.method}")

    # 保存结果
    df.to_parquet(args.output, index=False)
    print(f"特征工程完成，结果已保存到 {args.output}")

if __name__ == '__main__':
    main()