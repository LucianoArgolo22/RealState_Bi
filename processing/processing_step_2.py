#%%
from clients.ClientProcessingCleaning import Processing, pd
from clients.ClientSesMysql import ClientMysql
from clients.ClientLogger import ClientLogger
from clients.ClientMetrics import ClientMetrics

logger = ClientLogger(filename='dataset_processing_cleaner.log', app_name='processer_cleaner')
log = logger.get_log()


json_configs = [{
    'columns_to_segregate':{ 
                            'subzone1':'caba',
                            'subzone2':'capital federal'
                            },
    'values_to_unify':[
        {'subzone3': ['boca', 'belgrano', 'almagro', 'caballito', 'floresta', 'palermo', ('barrio ', 'mitre', ''), 'santa rita'
                      , 'nuñez', ('villa ', 'urquiza', ''), 'pompeya', ('mont', 'serrat', ''), 'paternal', ('micro', 'centro', '')]}
                        ]
    },
    {'columns_to_segregate':{ 
                            'subzone1':'gba-zona-norte',
                            'subzone2':'vicente lopez'
                            },
    'values_to_unify':[
        {'subzone3': [('villa ', 'martelli', ''), ('', 'villa adelina', ''), ('', 'vicente lopez', ''), 'olivos'
                      'munro', 'la lucila', {'value': ('', 'florida', ' vl'), 'filter': True}, 'carapachay']}
                        ]
    },
    {'columns_to_segregate':{ 
                            'subzone1':'gba-zona-norte',
                            'subzone2':'san-isidro'
                            },
    'values_to_unify':[
        {'subzone2': ['ninguno']}
                        ]
    },
        {'columns_to_segregate':{ 
                            'subzone1':'gba-zona-norte',
                            'subzone2':'pilar'
                            },
    'values_to_unify':[
        {'subzone2': ['pilar']},
        {'subzone3': ['zelaya', ('manuel ', 'alberti', '') ,('villa ', 'martelli', ''), ('villa ', 'verde', ''), ('villa ', 'rosa', ''),
                      'villa del lago', ('villa ', 'buide', ''), ('villa ', 'astolfi', ''), 'san alejo', ('presidente ', 'derqui', ''),
                      {'value': ('pilar ', 'pilar', ''), 'filter': True},
                      ('', 'pilar pil', 'ar'), {'value': ('', 'manzanares', ''), 'string_in_value': True}, 'fatima', ('la ', 'lonja', ''),
                      ('del ', 'viso', ''), {'value': ('', 'lagomarsino', ''), 'string_in_value': True}
                      ]}
                        ]
    },
        {'columns_to_segregate':{ 
                            'subzone1':'cordoba',
                            },
    'values_to_unify':[
        {'subzone2': ['capital']},
        {'subzone3': [{'value': ('', 'colon', ''), 'string_in_value': True}]}
                        ]
    },
            {'columns_to_segregate':{ 
                            'subzone1':'neuquen',
                            },
    'values_to_unify':[
        {'subzone2': ['neuquen']}
                        ]
    },
            {'columns_to_segregate':{ 
                            'subzone1':'mendoza',
                            },
    'values_to_unify':[
        {'subzone2': ['ninguno']}
                        ]
    }
    ]                                          
def processing_step_2(test:bool=False) -> None:
    environment:str = '' if not test else '_test' 
    
    mysql_object = ClientMysql(log=log, table=f'properties_stage_1{environment}')
    dict_ = mysql_object.generating_dict()
    df = pd.DataFrame(dict_)

    mysql_object2 = ClientMysql(log=log, table=f'properties_stage_2{environment}')
    dict_2 = mysql_object2.generating_dict()
    df2 = pd.DataFrame(dict_2)
    if not test:
        for config_json in json_configs:
            processor = Processing(df=df)
            #log.info('First ',len(df))
            df_ = processor.segregating_by_fields(columns_to_segregate=config_json['columns_to_segregate'])
            #log.info('Second ',len(df_))
            log.info(df_.subzone3.unique())
            for values in config_json['values_to_unify']:
                for column, values_to_be_found in values.items():
                    processor.combining_similar_values_df(column=column, to_find_values=values_to_be_found) 

            log.info(len(df_))
            log.info(sorted(df_.subzone3.unique()))
            #log.info('Third ',len(df_))
            df_ = df_.drop_duplicates(subset=['price', 'operacion', 'rooms', 'meters', 'subzone1', 'subzone2', 'subzone3'], keep='last')
            df_ = df_.drop_duplicates(subset=['Urls','price','expenses'], keep='last')
            df_ = df_[~df_.set_index(['Urls','Date']).index.isin(df2.set_index(['Urls','Date']).index)]
            #log.info('Fourth ',len(df_))
            mysql_object = ClientMysql(log=log, table='properties_stage_2', df=df_)
            mysql_object.inserting_rows()
            metrics = ClientMetrics()
            metrics.send_metrics('scrapper.pipeline_data', tags={'inserted_into':'properties_stage_2'}, fields={'new_data': len(df_)})
    else:
        log.info(len(df))
        log.info(sorted(df.subzone3.unique()))
        #log.info('Third ',len(df))
        df = df.drop_duplicates(subset=['price', 'operacion', 'rooms', 'meters', 'subzone1', 'subzone2', 'subzone3', 'Date'], keep='last')
        df = df.drop_duplicates(subset=['Urls','price','expenses'], keep='last')
        df = df[~df.set_index(['Urls','Date']).index.isin(df2.set_index(['Urls','Date']).index)]
        #log.info('Fourth ',len(df))
        mysql_object = ClientMysql(log=log, table='properties_stage_2_test', df=df)
        mysql_object.inserting_rows()
        metrics = ClientMetrics()
        metrics.send_metrics('scrapper.pipeline_data', tags={'inserted_into':'properties_stage_2_test'}, fields={'new_data': len(df)})


def processing_step_2_1(test:bool) -> None:
    mysql_object = ClientMysql(log=log, table=f'properties_stage_1')
    dict_ = mysql_object.generating_dict()
    df = pd.DataFrame(dict_)

    mysql_object2 = ClientMysql(log=log, table=f'properties_stage_2')
    dict_2 = mysql_object2.generating_dict()
    df2 = pd.DataFrame(dict_2)

    log.info(len(df))
    log.info(sorted(df.subzone3.unique()))
    #log.info('Third ',len(df))
    df = df.drop_duplicates(subset=['price', 'operacion', 'rooms', 'meters', 'subzone1', 'subzone2', 'subzone3', 'Date'], keep='last')
    df = df.drop_duplicates(subset=['Urls','price','expenses'], keep='last')
    df = df[~df.set_index(['Urls','Date']).index.isin(df2.set_index(['Urls','Date']).index)]
    #log.info('Fourth ',len(df))
    mysql_object = ClientMysql(log=log, table='properties_stage_2', df=df)
    mysql_object.inserting_rows()
    metrics = ClientMetrics()
    metrics.send_metrics('scrapper.pipeline_data', tags={'inserted_into':'properties_stage_2'}, fields={'new_data': len(df)})