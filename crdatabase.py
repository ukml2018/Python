import mysql.connector
from mysql.connector import errorcode
import mysql
import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy import exc
import sqlalchemy
import uuid
import logging
import logging.handlers
import datetime
import time
from decorators import Retry
from decorators import DBException
from functools import wraps
from LogHandler import Logger
import sys
#sys.path('C:\\IBM\\OpenShift')

##############
# simple CR class to handel database connections
#############

class crdatabase:

    def __init__(self):

        #path = os.path.abspath(__file__)
        #self.dir_path = os.path.dirname(path)
        #self.logger = Logger(self.dir_path + '/../webservice/mqtt/logs/', 'database', 'database').getlogger()

        self.DATABASE_NAME = "sampledb"
        self.DATABASE_PASS = "welcome1"

        #if os.getenv('MYSQL_HOST'):
        #   self.MYSQL_HOST = mysql-gamification.inmbzp8022.in.dst.ibm.com
        #   self.DATABASE_IP = os.getenv('MYSQL_HOST')
        #    self.DATABASE_PORTS = os.getenv('MYSQL_PORT')
        #    self.DATABASE_USER = os.getenv('MYSQL_USER')
        #else:
        self.MYSQL_HOST = 'mysql-gamification.inmbzp8022.in.dst.ibm.com'
        self.DATABASE_IP = '127.0.0.1'
        self.DATABASE_PORTS = '3306'
        self.DATABASE_USER = 'xxuser'
            
        self.connection()
    
    @Retry.retry(5, 10, 0)
    def connection(self):
        self.engine =create_engine('mysql+mysqlconnector://'+self.DATABASE_USER+':'+self.DATABASE_PASS+'@'+self.DATABASE_IP+':'+self.DATABASE_PORTS+'/'+self.DATABASE_NAME+'',pool_pre_ping = True)

    def _exception_handle(f, *args):
        @wraps(f)
        def wrapper(self, *args, **kwds):
            try:
                return f(self, *args, **kwds)
            except (exc.SQLAlchemyError, exc.DBAPIError) as err:
                print("type of the error is ", type(err))
                print("error is ", err)
                self.logger.exception("type of the error is ", type(err))
                self.logger.exception("error is ", err)
                if(isinstance(err,sqlalchemy.exc.OperationalError)):
                    self.logger.exception("inside Operational exception")
                    self.logger.exception("attempting to re-execute")
                    raise DBException
                    #self.dataframetomysql(df,strtable,strschema,straction,coltype)
                elif(isinstance(err,sqlalchemy.exc.DatabaseError)):
                    self.logger.exception("inside DatabaseError exception")
                    self.logger.exception("attempting to re-execute")
                    raise DBException
                elif(isinstance (err,sqlalchemy.exc.ProgrammingError)):
                    self.logger.exception("Access Denied, check User credentials " + str(err))
                else:
                    self.logger.exception("Unknown exception " + str(err))
        return wrapper

    @Retry.retry(5, 10, 0)
    @_exception_handle
    def dataframetomysql(self, df,strtable,strschema,straction,coltype=None):
            if coltype != None:
                df.to_sql(name=strtable, con=self.engine, if_exists=straction,index=False, dtype=coltype)
            else:
                df.to_sql(name=strtable, con=self.engine, if_exists=straction,index=False)
                
    @Retry.retry(5, 10, 0)  
    @_exception_handle         
    def dataframefrommysql(self, strsql):
            df=pd.read_sql(sql=strsql, con=self.engine)
            return df
    
       
    @Retry.retry(5, 10, 0)
    @_exception_handle
    def updatemysql(self,strsql):
        # using connection method to find errors
            strsql = ('select * from xxibm_product_catalog')
            conn = self.engine.connect()
            return conn.execute(strsql)

    #@Retry.retry(5, 10, 0)
    #@_exception_handle
    #def rulelog(self, classname, functionname, daymin, itemid, action, msguuid):
    #        loaduuid = uuid.uuid4()
    #        strsql = ('insert into rulelog (classname,functionname,daymin,itemid,action,msguuid,insert_ts,load_uuid) values(\'' + str(classname) + '\',\'' + str(functionname) + '\',\'' + str(daymin) + '\',\'' + str(itemid) + '\',\'' + str(action) + '\',\'' + str(msguuid) + '\',\'' + str(datetime.datetime.now()) + '\',\'' + str(loaduuid) + '\')')
    #        conn = self.engine.connect()
    #        conn.execute(strsql)
def main():
 print(updatemysql(self, strsql))
 if __name__ == '__main__':
    strsql = ('slect * from xxibm_product_catalog')
    print(updatemysql(self, strsql))
    main()