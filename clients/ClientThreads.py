import os
import pandas as pd
import datetime
from pandas import DataFrame
from clients.ClientSesMysql import ClientMysql


class ThreadsHandler:
    def __init__(self):
        pass

    def pooling_threads(self, *args) -> list:
        self.thread_pool = [thread for thread in args][0]
        
    def running_threads_pool(self) -> None:
        [thread.start() for thread in self.thread_pool]

    def freeing_threads(self) -> None:
        self.thread_pool = [thread for thread in self.thread_pool if thread.is_alive() ]

    def any_thread_over(self) -> None:
        return all([thread.is_alive() for thread in self.thread_pool])

class ObjectStorage:
    def __init__(self, log:object=None, load_path:str=None, full_path:str="\\utils\\Temp_Data\\") -> None:
        self.log = log
        self.load_path = load_path
        self.full_path = full_path

    def get_files(self) -> list:
        self.log.info('- ObjectStorage - get_files - Getting the path of local generated Data')
        full_path = f'{self.load_path}{self.full_path}'
        self.log.info(f'- ObjectStorage - get_files - Path :{full_path}')
        files = os.listdir(full_path)
        self.files = [full_path + file for file in files if 'DS_Store' not in file]
        return self.files

    def join_files(self, files: list) -> DataFrame:
        if files:
            self.log.info(f'- ObjectStorage - join_files - files found: {files}')
            df = pd.read_csv(files[0])
            df2 = pd.DataFrame(columns=df.columns)
            for file in files:
                df = pd.read_csv(file, index_col=False, dtype='str')
                df2 = pd.concat([df, df2])
            return df2
        self.log.info(f' - ObjectStorage - join_files - files not found')
        return pd.DataFrame()
    
    def drop_duplicates_ordered(self, df2) -> DataFrame:
        self.log.info(f'- ObjectStorage - drop_duplicates_ordered - dropping duplicates')
        df2 = df2.drop_duplicates()
        df2.reset_index(inplace=True)
        return df2

    def join_and_drop(self) -> DataFrame:  
        self.log.info(f'- ObjectStorage - join_and_drop - Joining files and starting to drop')
        files = self.get_files()  
        df = self.join_files(files=files)
        return self.drop_duplicates_ordered(df)
    
    def get_date(self, day=None) -> str:
        ddate = datetime.datetime.now().date()
        if not day:
            return str(ddate.year) + str(ddate.month)
        else:
            return str(ddate.year) + str(ddate.month) + str(ddate.day)

    def saved_unified_storages(self, database:bool=None, table:str=None) -> None:
        self.log.info(f'- ObjectStorage - saved_unified_storages - Saving unified storages')
        files = self.get_files()
        if files:
            df = self.join_and_drop().set_index('index')
            df = df.fillna('0')
            df = df[df.price.str.contains("[0-9,.]+")]
            self.log.info(f'- ObjectStorage - saved_unified_storages - Erasing files already joined')
            if database:    
                self.log.info(f'- ObjectStorage - saved_unified_storages - Using database')
                mysql_object = ClientMysql(log=self.log, table=table, df=df)
                mysql_object.inserting_rows()
            else:
                df.to_csv(f'{self.load_path}\\Data\\Derpatamentos_data_set_{self.get_date()}.csv', index=False, mode='a', header=False)
            self.erase_all_other_files()

    def erase_all_other_files(self) -> None:
        [os.remove(file) for file in self.files]