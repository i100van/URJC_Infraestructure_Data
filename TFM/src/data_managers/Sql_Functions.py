from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sshtunnel import SSHTunnelForwarder
import psycopg2
import psycopg2.extras
import logging
import cx_Oracle
import pyodbc
import numpy as np
import pandas as pd
import tqdm as tqdm
#TODO: Posible error, esto tiene .ex de windows
cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\i100v\PycharmProjects\TFM\src\data_managers\instantclient_19_12")

class SQL_Functions:
    def __init__(self,server,user,pss):
        self.server = server
        self.user = user
        self.pss = pss
        self.connection = None
        self.cursor = None
        self.connection_str = None
        self.ssh = None

    def connect_to_database(self, type, bbdd=None, port=None, service=None,ssh=dict()):
        if type=='SQL Server':
            self.connection_str = 'DRIVER={SQL Server};SERVER='+self.server+'; DATABASE='\
                             +bbdd+';UID='+self.user
            logging.info(f'SQL Server: Creating connection string {self.connection_str}')
            self.connection_str +=';PWD='+ self.pss
            self.connection = pyodbc.connect(self.connection_str)

        if type=='Oracle':
            logging.info(f'Oracle client version: {cx_Oracle.version}')
            self.connection_str = cx_Oracle.makedsn(host=self.server, port=port, service_name=service)
            logging.info(f'Creating connection Oracle {self.connection_str}')
            self.connection = cx_Oracle.connect(user=self.user, password=self.pss,
                                                dsn=self.connection_str, encoding="UTF-8")
        if type=='Postgre' and len(ssh.keys())==0:
            logging.info(f'Postgre: host={self.server} database={bbdd}')
            self.connection = psycopg2.connect(host=self.server,database=bbdd,port=port,
                                               user=self.user,password=self.pss)
            logging.info(f'Connection to Postgre Database done')

        if type=='Postgre' and len(ssh.keys())!=0:
            logging.info('Initialization SSH postgre conn')
            self.ssh=ssh
            logging.info(f"SSH params host:{self.ssh['host']} port: 22 key:{self.ssh['key']}"
                         f" user:{self.ssh['username']} binding: ({self.server}, {int(self.ssh['port'])})")

            tunnel = SSHTunnelForwarder(
                (self.ssh['host'], 22),
                allow_agent=False,
                ssh_password=self.ssh['key'],
                ssh_username=self.ssh['username'],
                remote_bind_address=('127.0.0.1', 5432),
                local_bind_address=('localhost', 3333)
            )
            tunnel.start()
            logging.info(f'Server connected via SSH')
            logging.info(f'Connecting to database server: {self.server} port: {port}'
                         f' database: {bbdd} user:{self.user}')
            self.connection = psycopg2.connect(host='127.0.0.1',database=bbdd,port=3333,
                                               user=self.user,password=self.pss)

        self.cursor = self.connection.cursor()

    def get_df_by_query(self,query):
        df = pd.read_sql(query, self.connection)
        return df

    def change_bbdd(self,bbdd:str):
        self.connection_str = 'DRIVER={SQL Server};SERVER='+self.server+'; DATABASE=' \
                              +bbdd+';UID='+self.user
        logging.info(f'SQL Server: Creating connection string {self.connection_str}')
        self.connection_str +=';PWD='+ self.pss
        self.connection = pyodbc.connect(self.connection_str)
        return None

    def add_df_to_table_to_sql(self,dft,schema,table):
        logging.info(f'Inserting df with shape: {dft.shape} into {schema}.{table}')
        if len(dft) > 0 and  len(dft) < 1000:
            splitter = len(dft)
        elif len(dft) < 10000:
            splitter = 1000
        elif len(dft) < 100000:
            splitter = 10000
        elif len(dft) > 100000:
            splitter = 100000
        for df in tqdm.tqdm(np.array_split(dft, len(dft)//splitter)):
            columns = ",".join(list(df))
            values = "VALUES({})".format(",".join(["%s" for _ in list(df)]))
            insert_stmt = "INSERT INTO {}.{} ({}) {}".format(schema,table,columns,values)
            psycopg2.extras.execute_batch(self.cursor, insert_stmt, df.values)
            self.connection.commit()
        logging.info(f'Inserted {dft.shape} to {schema}.{table}')
        return None

    def add_df_to_table(self,df,schema,table):
        path_file=f'../data/temporary/{table}_temp.csv'
        tablename=schema+'.'+table
        df.to_csv(path_file,index=False,header=False,sep='\t')
        logging.info(f'Output --> {df.shape}')
        f = open(path_file, 'r')
        self.cursor.copy_from(f, tablename, sep='\t')
        self.connection.commit()
        return None

    def execute_query(self,query):
        cursor=self.cursor
        cursor.execute(query)
        res = ''
        if not 'DEL' in query:
            while 1:
                row = cursor.fetchone()
                if not row:
                    break
                res = res + str(row)
        return res