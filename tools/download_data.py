import os
import sys
# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
from datetime import datetime
from utils.initializer import ComponentInitializer

def main():
    # 初始化组件
    init = ComponentInitializer()
    config = init.load_config()
    downloader = init.init_downloader()
    db = init.init_db()
    preprocessor = init.init_preprocessor()
    data_path = config.get('data_path', './data')  # 从配置获取data_path
    

    # 设置命令行参数
    parser = argparse.ArgumentParser(description='股票数据下载工具')
    parser.add_argument('--mode', type=int, required=True, 
                       choices=[1, 2, 3, 4],
                       help='1:更新stock_basic, 2:更新全量股票数据, 3:按日期更新, 4:导出数据')
    parser.add_argument('--date', type=str, 
                       help='指定日期(YYYYMMDD)，mode=3时使用')
    parser.add_argument('--output', type=str, default=os.path.join(data_path, 'stock_data.parquet'),
                       help='导出文件路径，mode=4时使用')
    args = parser.parse_args()

    if args.mode == 1:
        # 模式1: 更新stock_basic
        print("开始更新股票基本信息...")
        downloader.update_stock_basic()
        print("股票基本信息更新完成")

    elif args.mode == 2:
        # 模式2: 更新全量股票数据
        print("开始更新全量股票数据...")
        # 从stock_basic获取全量股票代码
        stocks = [row['ts_code'] for row in db.query("SELECT ts_code FROM stock_basic")]
        downloader.download_stocks(stock_codes=stocks)
        print("全量股票数据更新完成")

    elif args.mode == 3:
        # 模式3: 按日期更新
        date = args.date if args.date else datetime.now().strftime('%Y%m%d')
        print(f"开始更新{date}的股票数据...")
        downloader.update_daily_data(trade_date=date)
        print(f"{date}股票数据更新完成")

    elif args.mode == 4:
        # 模式4: 导出数据
        print(f"开始导出股票数据到{args.output}...")
        df = downloader.export_daily_to_parquet(
            output_path=args.output, 
            batch_size=config['data_config']['batch_size'], 
            code_prefixes=config['data_config']['code_prefixes']
        )
        df = preprocessor.preprocess_daily_data_basic(df)
        df.to_parquet(args.output, index=False)
        print("股票数据导出完成")

if __name__ == '__main__':
    main()
