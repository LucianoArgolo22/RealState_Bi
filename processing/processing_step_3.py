#%%
import pandas as pd
from clients.ClientSesMysql import ClientMysql
from clients.ClientLogger import ClientLogger

logger = ClientLogger(filename='dataset_processing_cleaner.log', app_name='processer_cleaner')
log = logger.get_log()

def processing_step_3():
    with open(r'C:\Users\Luciano\Desktop\Proyecto Scraper Bi\utils\Queries\ETL-processing_tables_stage_3.sql', 'r') as query:
        query = query.read()
        mysql_object = ClientMysql(log=log, table='properties_stage_2')
        dict_ = mysql_object.generating_dict(customized_query=query)
        df = pd.DataFrame(dict_)
        df = df.drop_duplicates()
        df = df.sort_values(by='expenses', ascending=True)
        df = df.drop_duplicates(subset=['price', 'rooms', 'meters', 'subzone1', 'subzone2', 'subzone3', 'Date'], keep='last')
        mysql_object = ClientMysql(log=log, table='properties_stage_3', df=df)
        mysql_object.inserting_rows()
