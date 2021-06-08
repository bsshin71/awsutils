from sqlalchemy import create_engine
import sys
from sqlalchemy.pool import NullPool
from config import Config

class DBConn:
    host: str
    port: str
    user: str
    password: str
    database: str

    """
    connection 정보를 채운다.
    connection 객체를 생성하낟.
    """
    def __init__(self, config):
        dbconfig = config.get_jsection('postgresql')
        self.host = dbconfig['host']
        self.port = dbconfig['port']
        self.user  = dbconfig['user']
        self.password = dbconfig['password']
        self.database = dbconfig['database']

        self.create_conn()

    """
    connection 객체를 생성한다.
    단일 연결이므로 connection pool 은 구성하지 않는다.
    """
    def create_conn(self):
        engine_conf = 'postgresql://{user}:{password}@{host}:{port}/{db}'.format(user=self.user
                                                                                 ,password=self.password
                                                                                 ,host=self.host
                                                                                 ,port=self.port
                                                                                 ,db=self.database )
        self.engine = create_engine(engine_conf,  poolclass=NullPool)

    """
    connection 을 하나 생성한다.
    """
    def get_conn(self):
        self.conn = self.engine.connect()
        return self.conn

    """
    connection 을 close한다.
    """
    def close(self):
        self.conn.close()

    """
    connection 을 close 한다.
    connection 객체를 destroy 한다.
    """
    def dispose(self):
        self.conn.close()
        self.engine.dispose()
