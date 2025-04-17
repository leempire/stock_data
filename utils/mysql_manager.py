import pymysql


class MySQLManager:
    """MySQL数据库管理类，用于执行查询、插入等操作
    
    功能包括：
    - 数据库连接管理
    - 执行SQL查询
    - 执行批量插入
    - 事务管理
    
    示例:
        >>> db = MySQLManager(host='localhost', user='root', 
        ...                  password='123456', database='stock_data')
        >>> results = db.query("SELECT * FROM stock_basic")
    """
    
    def __init__(self, host: str, user: str, password: str, 
                 database: str, port: int = 3306, charset: str = 'utf8mb4'):
        self.conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            charset=charset,
            cursorclass=pymysql.cursors.DictCursor
        )
        
    def query(self, sql: str, params: tuple = None):
        with self.conn.cursor() as cursor:
            cursor.execute(sql, params or ())
            return cursor.fetchall()
            
    def execute(self, sql: str, params: tuple = None):
        with self.conn.cursor() as cursor:
            affected_rows = cursor.execute(sql, params or ())
            self.conn.commit()
            return affected_rows
            
    def executemany(self, sql: str, params: List[tuple]):
        with self.conn.cursor() as cursor:
            affected_rows = cursor.executemany(sql, params)
            self.conn.commit()
            return affected_rows
            
    def close(self):
        if self.conn.open:
            self.conn.close()
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
