# -*- coding: utf-8 -*-
import psycopg2
import logging
from config import Config

LIMIT_RETRIES = 5
class Databases():
    def __init__(self, config ):
        dbconfig = config.get_jsection('postgresql')
        self.host = dbconfig['host']
        self.port = dbconfig['port']
        self.user = dbconfig['user']
        self.password = dbconfig['password']
        self.database = dbconfig['database']
        self.db = None
        self.cursor = None

        self.connect()

    def __del__(self):
        self.db.close()
        self.cursor.close()

    def executequery(self,query,args={}):
        self.cursor.execute(query,args)
        row = self.cursor.fetchall()
        return row

    def execute(self,query,args={}):
        self.cursor.execute(query,args)
        rowscount = self.cursor.rowcount

        return rowscount

    def connected(self) -> bool:
        return self.db and self.db.closed == 0

    def close(self):
        if self.connected():
            try :
                self.db.close()
            except Exception:
                pass
        self.db = None

    def connect(self, retry_counter=0):
        if not self.db:
            try:
                self.db = psycopg2.connect(host=self.host, dbname=self.database, user=self.user, password=self.password,
                                           port=self.port)
                self.cursor = self.db.cursor()
            except psycopg2.OperationalError as error:
                if not self.reconnect or retry_counter >= LIMIT_RETRIES:
                    raise error
                else:
                    retry_counter += 1
                    logging.error("got error {}. reconnecting {}".format(str(error).strip(), retry_counter))
                    time.sleep(2)
                    self.connect(retry_counter)
            except (Exception, psycopg2.Error) as error:
                raise error

    def reconnect(self):
       if not self.connected():
           logging.warn('Now going to reconnect to database')
           self.close()
           self.connect()

    def retryexecute(self, query, retrynum, args={}):
        rowscount = -1
        for i in range(retrynum):
            try:
                self.execute( query, args )
                rowscount = self.cursor.rowcount
            except (psycopg2.DatabaseError, psycopg2.OperationalError) as ex:
                logging.error(ex)
                logging.error('failed to execute query and retry it again : ' + query )
                self.reconnect()
                continue
            except (Exception, psycopg2.Error) as error:
                logging.error(error)
                break
            else:
                break
        else:
            logging.error('failed to execute query  and retry it again but eventually all try failed  : ' + query)
        return rowscount

    def commit(self):
        self.db.commit()

def main():
    pg = Databases()
    rows = pg.execute('select * from BNC_TRADE limit 10')

    for row in rows:
        print(row)

if __name__ == '__main__':
    main()
