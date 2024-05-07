#%%
from clients.ClientSesMysql import ClientMysql
from clients.ClientLogger import ClientLogger


logger = ClientLogger(filename='dataset_processing_cleaner.log', app_name='processer_cleaner')
log = logger.get_log()

def processing_step_1(test:bool=False):
    environment:str = '' if not test else '_test' 
    etl = f"ETL-processing_tables{environment}.sql"
    with open(f'C:\\Users\\Luciano\\Desktop\\Proyecto Scraper Bi\\utils\\Queries\\{etl}', 'r') as query:
        query = query.read()
        mysql_object = ClientMysql(log=log, table=f'properties_stage_1{environment}')
        mysql_object.execution_queries(query=query, execution_only=True )


