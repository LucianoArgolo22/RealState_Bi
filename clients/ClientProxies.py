import pandas as pd
import requests
import traceback 
import random
from fake_useragent import UserAgent
from clients.ClientCloudScrapper import Browser

TIMEOUT=15

class ClientProxies:
    def __init__(self, path:str, log:object, use_proxies:bool=False, cloud_scrapper:bool=False):
        self.path = path
        self.df = pd.read_csv(f'{path}\\utils\\proxies_filtered.csv')
        self.log = log
        self.ua = UserAgent()
        self.timeout = TIMEOUT
        self.use_proxies = use_proxies
        self.cloud_scrapper = cloud_scrapper

    def proxies_setter(self, proxi) -> None:
        if proxi:
            self.proxies = {
                'http': f'http://{proxi}',
                'https': f'http://{proxi}',
                }
        else:
            self.proxies = None

    def proxy_iterator(self) -> list:
        return list(self.df['proxy'])
    
    def proxy_update_setter(self, proxi:str) -> None:
       self.df = self.df[self.df['proxy'] != proxi]

    def proxy_erasing_from_df(self, proxi:str) -> None:
       self.proxy_update_setter(proxi)
       self.df.to_csv(f'{self.path}\\utils\\proxies_filtered.csv', index=False)

    def request_validator(self, status=403) -> bool:
        return status != 403

    def request_process(self, url) -> tuple:
        headers = {'user-agent': self.ua.random} 
        total_proxies = self.proxy_iterator() if len(self.proxy_iterator()) > 0 else [None]
        
        #if 10 > len(total_proxies):
        #    self.log.warning(f'Running out of proxies, actually : {len(total_proxies)}')
        
        while total_proxies:
            proxi = random.choice(total_proxies)
            self.proxies_setter(proxi)
            try:
                #{iteration} at the end of the url for argenprop gives me back a 403, so i just replace it
                #if it's iterating over a page, then it doesn't have {iteration} in the url
                #so it's solving the problem
                modified_url = url.replace('{iteration}', '')
                if self.use_proxies:
                    r = requests.get(modified_url, headers = headers, proxies = self.proxies, timeout=self.timeout) 
                else:
                    if self.cloud_scrapper:
                        brws = Browser()
                        r = brws.get(modified_url)
                    else:
                        r = requests.get(modified_url, headers = headers) 
                if self.request_validator(r.status_code):
                    self.log.info(f' ClientProxies - request_process - Using proxies: {self.use_proxies} | Cloud Scrapper: {self.cloud_scrapper} | Url: {modified_url} | status: {r.status_code}')
                    return r, r.status_code
                else:
                    #self.proxy_erasing_from_df(proxi)
                    break
                    #self.log.info(f'Proxy erased from dataset because its not working or either its too slow: {proxi}')
                    self.df = pd.read_csv(f'{self.path}\\utils\\proxies_filtered.csv', index=False)
                
            except:
                if 'Connection refused' in traceback.format_exc().splitlines()[-1]:
                    self.log.info(f'Using proxi: {proxi}')
                    self.proxy_erasing_from_df(proxi)
                    #self.log.info(f'Proxy erased from dataset because its not working or either its too slow: {proxi}')
                self.log.warning(f' - Proxi - request_process - Internal Thread Error ----- {"  ----  ".join(traceback.format_exc().splitlines()[-2:][-40:])}')


class ClientProxiesFilter:
    def __init__(self, log:object, path:str=None):
        self.path:str = path
        self.proxies_file:str = 'proxies'
        self.log = log
        self.timeout = TIMEOUT

    def proxy_iterator(self) -> tuple:
        """
        Read in a CSV file of proxy servers located at the specified path and return a tuple containing a list of the proxy servers in reverse order, and the entire pandas DataFrame representing the CSV file.

        Returns
        -------
        tuple
            A tuple containing a list of proxy servers in reverse order and the pandas DataFrame representing the CSV file.

        """
        df = pd.read_csv(f'{self.path}\\utils\\{self.proxies_file}.csv')
        return list(df['proxy']), df

    def proxy_filter(self) -> None:
        ua = UserAgent()
        status = 0
        (proxies_list, df) = self.proxy_iterator()
        for proxi in proxies_list:
            proxies = {
            'http': f'http://{proxi}',
            'https': f'http://{proxi}',
            }
            try:
                text = requests.get('https://www.argenprop.com/departamento-alquiler-barrio-belgrano-2-dormitorios', headers={'user-agent': ua.random}, proxies=proxies, timeout=self.timeout)
                status = text.status_code
                if status == 200:

                    #adding new proxy to filtered proxies
                    df3 = df[df['proxy'] == proxi]
                    self.log.info(f'Status : {status} || Proxy Working : {proxi}')
                    df3.to_csv(f'{self.path}\\utils\\{self.proxies_file}_filtered.csv', index=False, mode='a', header=False)
                    
                    #erasing proxy already filtered
                    df = df[df['proxy'] != proxi]
                    df.to_csv(f'{self.path}\\utils\\{self.proxies_file}.csv', index=False)
            except:
                #erasing proxy not working
                df = df[df['proxy'] != proxi]
                df.to_csv(f'{self.path}\\utils\\{self.proxies_file}.csv', index=False)
