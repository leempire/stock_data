from .config import load_config
from .mysql_manager import MySQLManager
from .tushare_api import TushareAPI
from .stock_downloader import StockDownloader
from .data_preprocessor import DataPreprocessor

class ComponentInitializer:
    """组件初始化器，支持按需加载组件"""
    
    def __init__(self):
        self._config = None
        self._db = None
        self._tushare_api = None
        self._downloader = None
        self._preprocessor = None
        
    def load_config(self):
        """加载配置文件"""
        if self._config is None:
            self._config = load_config()
        return self._config
    
    def init_db(self):
        """初始化数据库连接"""
        if self._db is None:
            config = self.load_config()
            self._db = MySQLManager(**config['db_config'])
        return self._db
    
    def init_tushare_api(self):
        """初始化Tushare API"""
        if self._tushare_api is None:
            config = self.load_config()
            self._tushare_api = TushareAPI(
                data_path=config['data_path'],
                token=config['tushare_token']
            )
        return self._tushare_api
    
    def init_downloader(self):
        """初始化股票下载器"""
        if self._downloader is None:
            db = self.init_db()
            tushare_api = self.init_tushare_api()
            self._downloader = StockDownloader(db, tushare_api)
        return self._downloader

    def init_preprocessor(self):
        """初始化数据预处理模块"""
        if self._preprocessor is None:
            self._preprocessor = DataPreprocessor()
        return self._preprocessor