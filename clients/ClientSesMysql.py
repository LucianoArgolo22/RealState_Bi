import mysql.connector
import pandas as pd
import typing
import os 


def connector() -> object:
    if bool(os.getenv('MYSQL_CONN')):
        conn = mysql.connector.connect(host=os.getenv('MYSQL_HOST'),
                        user=os.getenv('MYSQL_USER'),
                        password=os.getenv('MYSQL_PASSWORD'))
        return conn
    else:
        conn = mysql.connector.connect(host='localhost',
                        user='root',
                        password='password')
        return conn



class ClientMysql:
    def __init__(self, log:object, table:str = None, df:pd.DataFrame = None):
        self.log = log
        self.conn = connector()
        self.df = df
        self.table = table

    def infering_columns(self) -> list:
        cursor2 = self.conn.cursor()
        query = f"describe challenge.{self.table}"
        cursor2.execute(query)
        records = cursor2.fetchall()
        df = pd.DataFrame(records)
        self.log.info(f'Infered colums:{list(df[0])}')
        return list(df[0])

    def generating_fields(self) -> str:
        self.log.info(f'Generating fields:{self.infering_columns()}')
        return ','.join(self.infering_columns())

    def generating_values(self) -> str:
        self.log.info('Generating Values')
        return ','.join(['%s' for _ in range(len(self.df.keys()))])

    def insert_query(self) -> str:
        query = f"""INSERT into challenge.{self.table} ({self.generating_fields()})
                                        VALUES ({self.generating_values()})"""
        self.log.info(f'Generating Query: {query}')
        return query

    def batch_insert(self) -> list:
        records_to_insert:list = []
        self.log.info(f'Inserting Batch')
        for values in self.df.values:
            records_to_insert.append(tuple(values))
        return records_to_insert

    def inserting_rows(self) -> None:
        insert_query = self.insert_query()
        records_to_insert = self.batch_insert()
        cursor = self.conn.cursor()
        self.log.info(f'Inserting records')
        try:
            while records_to_insert:
                if len(records_to_insert) > 2000:
                    cursor.executemany(insert_query, records_to_insert[0:2000])
                    records_to_insert = records_to_insert[2000:]
                else:
                    cursor.executemany(insert_query, records_to_insert)
                    records_to_insert = []
            self.conn.commit()
            cursor.close()
            self.conn.close()
        except Exception as e:
            for _ in range(300):
                self.log.info(f' ClientMysql - Inserting Rows -- {e}')
    
    def read_query(self) -> str:
        query = f"""select * from challenge.{self.table}"""
        self.log.info(f'Generating Query: {query}')
        return query
    
    def reading_rows(self) -> dict:
        cursor = self.conn.cursor()
        query = self.read_query()
        self.log.info(f'Reading records')
        cursor.execute(query)
        records = cursor.fetchall()
        self.log.info(f'Total records read :{len(records)}')
        cursor.close()
        self.conn.close()
        return records

    def delete_query(self,) -> str:
        query = f"""delete from challenge.{self.table}"""
        self.log.info(f'Generating Query: {query}')
        return query

    def deleting_table(self) -> None:
        cursor = self.conn.cursor()
        query = self.delete_query()
        self.log.info(f'Deleting records')
        cursor.execute(query)
        self.conn.commit()


    def restore_backup(self) -> None:
        self.deleting_table()
        self.inserting_rows()
        self.log.info(f'Restoring Table from Backup')
        

    def df_setter(self, df:pd.DataFrame) -> None:
        self.df = df

    def execution_queries(self, query:str, execution_only:bool=None) -> list:
        cursor = self.conn.cursor()
        cursor.execute(query)
        if execution_only:
            self.log.info(f'Query Executed')
            self.conn.commit()
            cursor.close()
            return None
        self.log.info(f'Reading records')
        records = cursor.fetchall()
        self.log.info(f'Total records read :{len(records)}')
        return records

    def getting_name_columns(self) -> list:
        query = f'describe challenge.{self.table}'
        result =  self.execution_queries(query)
        return [values[0] for values in result]

    def getting_rows(self, customized_query:str=None) -> list:
        query = f'''select * from challenge.{self.table}
                where date >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 5 DAY), '%Y/%m/%d')'''
        if customized_query:
            query = customized_query
        result =  self.execution_queries(query)
        return [list(values) for values in result] if len(result) > 0 else [['']]

    def generating_dict(self, customized_query:str=None) -> dict:
        names = self.getting_name_columns()
        self.log.info(f' - Client Mysql - generating_dict - Names - length: {len(names)}  and names :{names}')
        rows = self.getting_rows(customized_query=customized_query)
        self.log.info(f' - Client Mysql - generating_dict - Values - length: {len(rows[0])}  and values :{rows[0]}')
        self.log.info(f' - Client Mysql - generating_dict - Total records read :{len(rows)}')
        if len(names) == len(rows[0]):
            dict_ = {name: [] for name in names}
            for row in rows:
                for i, name in enumerate(names):
                    dict_[name].append(row[i])
            self.conn.close()
            return dict_
        else:
            self.log.warning(f' - Client Mysql - generating_dict - Table might be empty')
            self.log.warning(f' - Client Mysql - generating_dict - Columns are not equal to rows : columns {names}, rows {rows[0]}')
            return {name: [] for name in names}
