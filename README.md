# stock_data

## 环境配置
1. 安装Python和相关依赖
```bash
pip install -r requirements.txt
```

2. 配置数据库连接：修改config_template.yaml文件中的数据库连接信息，并将其重命名为config.yaml

## 初次使用的初始化
```sql
source scripts/create_database.sql;
```

```bash
# 更新股票基本信息
python tools/download_data.py --mode 1
# 更新全量股票信息
python tools/download_data.py --mode 2
```

## 每天数据更新
```bash
# 按日期更新，默认当天
python tools/download_data.py --mode 3
# 或指定日期
python tools/download_data.py --mode 3 --date 20250416
# 导出数据
python tools/download_data.py --mode 4 --output ./data/stock_data.parquet
```
