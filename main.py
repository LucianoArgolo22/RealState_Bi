#%%
#sitio https://planetscale.com/docs
import time
import traceback 
import datetime
from services.facebook_posts_service import facebook_posts
from services.twitter_posts_service import twitter_posts 
from services.twitter_facebook_estadisticas_service import  *
from clients.ClientDataAggregations import EndToEndProcess
from clients.ClientLogger import ClientLogger
from clients.ClientThreads import ObjectStorage
from scrapper_cleaner_docker.scrapping.controllers.controller_scrapper import scrappingV2, sendingv2
from processing.pipeline import pipeline, daily_cleansing


if __name__ == '__main__':
    path = '\\'.join(__file__.split('\\')[:-1])
    date = ObjectStorage().get_date(day=True)
    logger = ClientLogger(f'{path}\\utils\\scrapper_{date}.log', 'scrapper_app')
    log = logger.get_log()
    log.info(f"Log Path --- {path}\\utils\\scrapper_{date}.log")


    # limpieza de datos viejos y backup de los nuevos
    # de esta forma las tablas no están saturadas de información para nuevos cálculos
    daily_cleansing(test=False)
    log.info('Daily Cleansing has ended')


    #obten la fecha del día de hoy
    day = datetime.datetime.now().date().day
    date = ObjectStorage().get_date(day=True)
    date_name = ObjectStorage().get_date(name_date=True)
    #transformala a lunes martes miercoles jueves


    # #generador del promedio de los alquileres
    EndToEndProcess.process_1(log=log, path=path, days_off=30)
    time.sleep(20) 
    
    # scrapping de propiedades en venta
    scrappingV2(log, path, 'properties_raw', 'scrapping/venta', location='/Scrapper/')
    
    for _ in range(15):
        log.info('Scrapping for venta has ended')

    time.sleep(120)
    
    for i in range(1, 46):
        log.info(f'Running iteration number {i}')

        if i % 1 == 0 and i % 24 != 0:
            scrappingV2(log, path, 'properties_raw', 'scrapping/alquiler', location='/Scrapper/')
            for _ in range(15):
                log.info('Scrapping for alquiler has ended')

            pipeline(test=False)
            log.info('ETL process ended, tables loaded')

            # BotGetUpdates(log=log, path=path)
            # log.info('Getting updates of users')

        if i == 1:   
            # el proceso 2 necesita que el proceso 1 se cargue nuevamente, sino no se carga con las fechas actuales,
            # dado que no posee fechas actuales que cargar, hasta que haya scrappeado de otros sitios
            # sin cargarse con fechas actuales el proceso 2 no obtiene datos (dado que corre para fecha actual)
            EndToEndProcess.process_1(log=log, path=path, days_off=30)
            EndToEndProcess.process_2(log=log, path=path)

        if i == 1 and day != 1:
            #posteo de estadísticas por zona y por cantidad de ambientes
            #se generan variaciones en ars anual y mensual
            posting_process(path=path, func=estadisticas_porcentuales_global,
                                tipo_operacion='alquiler', tipo_moneda='ARS')

        if i == 18 and (date_name != 'Saturday' or date_name != 'Sunday') and day != 1:
            #se generan variaciones en usd anual y mensual
            posting_process(path=path, func=estadisticas_porcentuales_global,
                                tipo_operacion='venta', tipo_moneda='USD')

        if (i == 8 and date_name == 'Saturday') or (i == 1 and day == 1):
            #se generan los promedios de los precios de ALQUILERES
            posting_process(path=path, func=estadisticas_valores,
                                tipo_operacion='alquiler', tipo_moneda='ARS')

        if (i == 14 and date_name == 'Sunday') or (i == 8 and day == 1):
            #se generan los promedios de los precios de VENTA
            posting_process(path=path, func=estadisticas_valores,
                                tipo_operacion='venta', tipo_moneda='USD')

        time.sleep(800)
