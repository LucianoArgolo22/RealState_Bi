#%%
from clients.ClientProcessingCleaning import Cleaning, pd
from clients.ClientSesMysql import ClientMysql
from clients.ClientLogger import ClientLogger
from datetime import datetime as dt, timedelta
from typing import Any
import numpy as np

# path = '\\'.join(__file__.split('\\')[:-2])
# logger = ClientLogger(filename='dataset_processing_cleaner.log', app_name='processer_cleaner')
# log = logger.get_log()
# cwd = '/'.join(__file__.split('/')[:-2])

class Aggregation:
    def __init__(self, path:str, log:Any, days_off:int, start_date:str, test:bool=False, reprocess:bool=True) -> None:
        self.environment:str = '' if not test else '_test' 
        self.reprocess:str = '' if not reprocess else '_bkup'
        self.start_date = start_date
        self.path = path
        self.log = log
        self.etl = f"ETL-analysis_and_metrics{self.environment}{self.reprocess}.sql"
        self.days_off = days_off
        self.df = self.get_df()
        
    def get_df(self) -> pd.DataFrame:
        mysql_object = ClientMysql(log=self.log, table=f'properties_agg_features{self.environment}')
        analytical_query = open(file=f'{self.path}/utils/Queries/{self.etl}', mode="r").read()
        dict_ = mysql_object.generating_dict( customized_query=analytical_query.format( days_off=self.days_off, start_date=self.start_date))
        df = pd.DataFrame(dict_)
        df.columns = df.columns.str.lower()
        #df.to_csv(r'C:\Users\Luciano\Desktop\Proyecto Scraper Bi\cleaning_dataset\utils\input_data.csv')
        self.log.info(f'Cleaning and Analysis - Aggregation - get_df - the length of data frame obtained is: {len(df)}')
        return df

    def seggregating_by_meters(self) -> None:
        bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 100, 150, 200, 300, float ('inf')] 
        labels = [0, 1, 2, 3, 4, 5, 6, 7, 8 ,9 ,10 ,11 ,12]
        self.df['meters_bin'] = pd.cut(self.df['meters'], bins=bins, labels=labels)

    def filter_data(self, *args) -> None:
        # example of kwargs 
        # kwargs = {'operacion': 'alquiler', 'currency': 'ARS'}
        # filter_data(**kwargs)
        for query in args:
            # Filter the dataframe by the key and value
            self.df = self.df.query(query)
            self.log.info(f'Cleaning and Analysis - Aggregation - filter_data - \
                           data filtered for query: {query} \
                           , length obtained: {len(self.df)}')
        
    def convert_data(self, **kwargs):
    # Loop through the key-value pairs in the kwargs dictionary
    # Create a dictionary of kwargs with the desired columns and data types
    # kwargs = {'price': 'int', 'expenses': 'int', 'meters': 'int', 'rooms': 'int'}
    # Call the convert_data function with the input dataframe and kwargs dictionary
    # convert_data(**kwargs)
        for key, data_type in kwargs.items():
            # Convert the column to the specified data type
            self.df[key] = self.df[key].astype(data_type)
        # Return the converted dataframe
        
    def process_data(self, fields:list) -> None:
        # Initialize an empty list for storing the processed data
        processed_data = []
        # Initialize a counter variable
        i = 0
        # Loop through the values of the first field
        for value1 in sorted(list(self.df[fields[0]].unique())):
            # Filter the dataframe by the first field value
            sub_data1 = self.df[self.df[fields[0]] == value1]
            # Loop through the values of the second field
            for value2 in sorted(list(self.df[fields[1]].unique())):
                # Filter the dataframe by the second field value
                sub_data2 = sub_data1[sub_data1[fields[1]] == value2]
                # Loop through the values of the third field
                for value3 in sorted(list(self.df[fields[2]].unique())):
                    # Filter the dataframe by the third field value
                    sub_data3 = sub_data2[sub_data2[fields[0]] == value3]
                    #print(f'date ---> ',value1, value2, value3)
                    # Clean the data using the Cleaning class methods with hardcoded parameters
                    sub_data3 = Cleaning.outliers_filter(df=sub_data3, filter_columns=['price'], std_dev=1)
                    sub_data3 = Cleaning.cleaning_df_by_quantiles(df=sub_data3, next_jump=1, lesser_quantile=0.05, bigger_quantile=0.95 ,columns=['meters'])
                    # Append the cleaned data to the processed data list
                    if not sub_data3.empty:
                        processed_data.append(sub_data3)
                    # Increment the counter variable
                    i += 1
        # Concatenate the processed data list into a single dataframe
        if len(processed_data) > 0:
            processed_data = pd.concat(processed_data)
            # Return the processed data dataframe
            self.df = processed_data
        else:
            self.df = pd.DataFrame()

    def whole_process(self, filter_list:list=['operacion =="alquiler"', 'currency =="ARS"']) -> None:
        self.seggregating_by_meters()
        self.filter_data(*filter_list)
        convert_dict = {'price': 'int', 'expenses': 'int', 'meters': 'int', 'rooms': 'int'}
        self.convert_data(**convert_dict)
        fields = ['rooms', 'subzone3', 'meters_bin']
        self.process_data(fields=fields)
        if not self.df.empty:
            self.df = Cleaning.cleaning_df_by_quantiles(df=self.df, next_jump=1, lesser_quantile=0.01, bigger_quantile=0.98 ,columns=['price'])
            self.df = Cleaning.cleaning_df_by_quantiles(df=self.df, next_jump=1, lesser_quantile=0.02, bigger_quantile=0.98 ,columns=['meters'])
            self.convert_data(**convert_dict)
        else :
            self.log.info(f'Cleaning and Analysis - Aggregation - whole_process - Dataframe is empty')


class EndToEndProcess:
    @staticmethod
    def reprocess(path:str, start_date:str='2021-08-01', end_date:str=None, log:Any=None, days_off:int=30) -> None:
        #generate iteration that adds one day to an specefic date and runs the process till reaching actual date
        start_date = dt.strptime(start_date, '%Y-%m-%d').date()
        current_date = dt.strftime(dt.today().date(), "%Y-%m-%d")
        current_date = dt.strptime(current_date, '%Y-%m-%d').date()
        if end_date:
            current_date = dt.strptime(end_date, '%Y-%m-%d').date()
        else:
            current_date = dt.strftime(dt.today().date(), "%Y-%m-%d")
            current_date = dt.strptime(current_date, '%Y-%m-%d').date()
        while start_date < current_date:
           start_date_str = str(dt.strftime(start_date, "%Y-%m-%d"))
           log.info(f'EndToEndProcess - reprocess - Running process for date {start_date_str}')
           EndToEndProcess.process_1(log=log, path=path, days_off=days_off,
                                      test=False, start_date=start_date_str,
                                      reprocess=True)
           start_date = start_date + timedelta(days=1)

    @staticmethod
    def process_1(log:Any, path:str, days_off:int, test:bool=False, start_date:str=None, reprocess:bool=False) -> None:
        """
        This method is in charge of processing the data for the first part of the process
        """
        bkup = '' if not reprocess else '_bkup'
        environment:str = '' if not test else '_test' 
        current_date = str(dt.strftime(dt.today().date(), "%Y-%m-%d")) 
        #i need to substract 30 days to current date dt.strftime(dt.today().date()
        current_date_str = current_date if not start_date else start_date 
        current_date = dt.strptime(current_date_str, '%Y-%m-%d').date()

        dates_off_date = str(dt.strftime(current_date - timedelta(days=days_off), "%Y-%m-%d"))
        #processing and cleaning outliers 
        log.info("EndToEndProcess - process_1 - Starting whole processing for aggregation")
        #erasing info to avoid duplication of data for current day
        mysql_object = ClientMysql(log=log, table=f'properties_metrics{environment}', schema='data_science')
        mysql_object.execution_queries(query=f"delete from data_science.properties_metrics{environment} where date = '{current_date_str}'", execution_only=True)
        mysql_object.execution_queries(query=f"delete from data_science.filtered_data{environment} where date = '{current_date_str}'", execution_only=True)


        log.info(f' ClientDataAggregations - process 1 - Data erased for current date -{current_date_str}- for data_science.properties_metrics{environment}')

        #filtros a procesar
        filters:list = [
                ['operacion =="alquiler"', 'currency =="ARS"',
                'meters >0', 'rooms >0 & rooms <=7', 'price > 30000',
                'subzone1 !="No Especifica"', 'subzone2 !="No Especifica"',
                'subzone3 !="No Especifica"'],
                ['operacion =="alquiler"', 'currency =="USD"',
                'meters >0', 'rooms >0 & rooms <=7', 'price > 10', 'price < 11000',
                'subzone1 !="No Especifica"', 'subzone2 !="No Especifica"',
                'subzone3 !="No Especifica"'],
                ['operacion =="venta"', 'currency =="USD"',
                'meters >0', 'rooms >0 & rooms <=7', 'price > 10000', 'price < 1000000',
                'subzone1 !="No Especifica"', 'subzone2 !="No Especifica"',
                'subzone3 !="No Especifica"']
                ]
        for filter_list in filters:
            agg = Aggregation(path=path, log=log, days_off=days_off, test=test, reprocess=reprocess, start_date=current_date_str)
            agg.whole_process(filter_list=filter_list) 
            if not agg.df.empty:
                df = agg.df
                log.info(f"EndToEndProcess - process - data obtained from aggregation : {len(df)}")

                #se dropea el "meters_bin" antes de ser guardado en la tabla filtered_data
                #así los datos tienen el mismo formato que de donde vienen solo que están filtrados
                df2 = df.drop(columns=['meters_bin'])
                
                #i need to filter df2, and get another df filtered by the variable "current_date_str"
                filtered_df = df2[df2['date'] == current_date_str]

                #se guarda la información filtrada para posterior uso antes de ser agrupada
                mysql_object = ClientMysql(log=log, table=f'filtered_data{environment}', schema='data_science', df=filtered_df)
                mysql_object.inserting_rows()

                df_agg = df[['price', 'rooms', 'subzone1', 'subzone2','subzone3', 'operacion', 'currency']]
                df_grouped = df_agg.groupby(['rooms', 'subzone1',
                    'subzone2', 'subzone3', 'operacion', 'currency']).agg([np.mean, np.median, 'count']).reset_index()
                df_grouped.columns = ['rooms',  'subzone1', 'subzone2', 'subzone3', 'operacion', 'currency', 'price_mean', 'price_median', 'total_count']
                #filtering data that is not actually helping to get a good mean or median
                df_grouped = df_grouped.query('total_count >= 6')
                df_grouped = df_grouped.sort_values(by=[('price_mean')], axis=0, ascending=False)
                df_grouped = df_grouped.dropna()
                df_grouped = df_grouped.reset_index(drop=True)
                len(df_grouped)

                df_grouped.sort_values('price_mean', ascending=False)
                df_grouped['date'] = current_date_str
                df_grouped['meters_bin'] = 0

                df_grouped.columns

                df_grouped = df_grouped[['rooms', 'meters_bin', 'subzone1', 'subzone2', 'subzone3', 'operacion', 'currency', 'price_mean', 'price_median', 'date']]

                #if the process already ran before in the same day, let's not duplicate data

                #saving dataframe into db
                df_grouped = df_grouped.drop_duplicates()
                #df_grouped.to_csv(r'C:\Users\Luciano\Desktop\Proyecto Scraper Bi\cleaning_dataset\utils\output_data.csv')
                df_grouped.to_csv(r'C:\Users\Luciano\Desktop\Proyecto Scraper Bi\cleaning_dataset\test_datagrouped_data_for_metrics.csv', index=False)
                mysql_object = ClientMysql(log=log, table=f'properties_metrics{environment}', schema='data_science', df=df_grouped)
                mysql_object.inserting_rows()
            else:
                log.info(f"EndToEndProcess - process - data obtained from aggregation is empty : {len(agg.df)}")

        log.info(f' ClientDataAggregations - process 1 - Data loaded into data_science.properties_metrics{environment}{bkup}')

    @staticmethod
    def query_executor(log:Any, query:str, tipo_operacion:str=None, tipo_moneda:str=None) -> None:
        with open(f'C:\\Users\\Luciano\\Desktop\\Proyecto Scraper Bi\\utils\\Queries\\{query}.sql', 'r') as query:
            query = query.read()
            if tipo_operacion and tipo_moneda:
                query = query.format(tipo_operacion=tipo_operacion, tipo_moneda=tipo_moneda)
            mysql_object = ClientMysql(log=log)
            mysql_object.execution_queries(query=query, execution_only=True)

    @staticmethod
    def process_2(log:Any, path:str, test:bool=False) -> None:
        environment:str = '' if not test else '_test' 
        current_date = str(dt.strftime(dt.today().date(), "%Y-%m-%d"))
        #processing and cleaning outliers 
        log.info("EndToEndProcess - process_2 - Starting whole processing for statistics estimation")
        #erasing info to avoid duplication of data for current day
        mysql_object = ClientMysql(log=log, table=f'properties_statistics{environment}', schema='data_science')
        mysql_object.execution_queries(query=f"delete from data_science.properties_statistics{environment} where fecha_actual = '{current_date}'", execution_only=True)
                
        log.info(f' ClientDataAggregations - process 2 - Data erased for current date -{current_date}- for data_science.properties_statistics{environment}')
        
        #se generan las métricas de los datos
        EndToEndProcess.query_executor(log=log, query='ETL-Bot_estadisticas_global', tipo_operacion='alquiler', tipo_moneda='ARS')
        EndToEndProcess.query_executor(log=log, query='ETL-Bot_estadisticas_global', tipo_operacion='venta', tipo_moneda='USD')
        EndToEndProcess.query_executor(log=log, query='ETL-Bot_estadisticas_global_subzone1', tipo_operacion='alquiler', tipo_moneda='ARS')
        EndToEndProcess.query_executor(log=log, query='ETL-Bot_estadisticas_global_subzone1', tipo_operacion='venta', tipo_moneda='USD')
        EndToEndProcess.query_executor(log=log, query='ETL-Bot_estadisticas_global_subzone1_subzone2', tipo_operacion='alquiler', tipo_moneda='ARS')
        EndToEndProcess.query_executor(log=log, query='ETL-Bot_estadisticas_global_subzone1_subzone2', tipo_operacion='venta', tipo_moneda='USD')


        #falta generar bien los datos de fondo por detrás para que el promedio de de forma coherente 
        #EndToEndProcess.query_executor(log=log, query='ETL-Bot_estadisticas_global', tipo_operacion='alquiler', tipo_moneda='USD')
        log.info(f' ClientDataAggregations - process 2 - Data loaded into data_science.properties_statistics{environment}')



        # se generan datos de los metros cuadrados en pesos y en USD
        mysql_object = ClientMysql(log=log, table=f'properties_squared_meters_price{environment}', schema='data_science')
        mysql_object.execution_queries(query=f"delete from data_science.properties_squared_meters_price{environment} where date = '{current_date[:7]}'", execution_only=True)
                
        EndToEndProcess.query_executor(log=log, query='ETL-squared_meters', tipo_operacion='alquiler', tipo_moneda='ARS')
        EndToEndProcess.query_executor(log=log, query='ETL-squared_meters', tipo_operacion='venta', tipo_moneda='USD')
        log.info(f' ClientDataAggregations - process 2 - Data loaded into data_science.properties_squared_meters_price{environment}')



        # se generan datos de la proporción entre venta y alquiler
        mysql_object = ClientMysql(log=log, table=f'properties_squared_meters_price{environment}', schema='data_science')
        mysql_object.execution_queries(query=f"delete from data_science.properties_proportions_between_sell_and_rent{environment} where date = '{current_date[:7]}' ", execution_only=True)
        
        EndToEndProcess.query_executor(log=log, query='ETL-Proportions_between_sell_and_rent')
        log.info(f' ClientDataAggregations - process 2 - Data loaded into data_science.properties_proportions_between_sell_and_rent{environment}')
