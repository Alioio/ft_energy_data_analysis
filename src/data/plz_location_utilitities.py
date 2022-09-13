from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import time
from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd
from lxml import etree
import re
import os
from datetime import datetime
from collections import deque
import json

class PLZ_location_utility:
    def __init__(self, energy_type='electricity', consumption=3000, plz_list=[73033]):
        self.energy_type = energy_type
        self.init_parameters()
        self.consumption = consumption
        self.chrome_options = Options()
        self.chrome_options.headless = True
        self.every_plz_df = pd.DataFrame()
        self.plz_list = deque(plz_list)
        self.plz_places_id_dict = dict()
        self.plz_places_dict = dict()

    def init_parameters(self):
        gas_url = 'https://www.finanztip.de/gaspreisvergleich/'
        electricity_url = 'https://www.finanztip.de/stromvergleich/'

        # regularity:
        x_path_gas_regularity = '//*[@id="c84990"]/div/section/form/div[2]/div[2]/div[1]/div[3]/label'
        x_path_electricity_regularity = '//*[@id="c84995"]/div/section/form/div[2]/div[2]/div[1]/div[3]/label'

        # plz:
        x_path_gas_plz =         '//*[@id="c84990"]/div/section/form/div[2]/div[1]/div[1]/div/div[2]/input'
        x_path_electricity_plz = '//*[@id="c84995"]/div/section/form/div[2]/div[1]/div[1]/div/div[2]/input'

        # plz error:
        x_path_gas_plzerror =         '//*[@id="c84990"]/div/section/form/div[2]/div[1]/div[1]/div/div[2]/div'
        x_path_electricity_plzerror = '//*[@id="c84995"]/div/section/form/div[2]/div[1]/div[1]/div/div[2]/div'

        # consumption:
        x_path_gas_consumption = '//*[@id="gas-tool-calculator__yearly-consumption"]'
        x_path_electricity_consumption = '//*[@id="power-tool-calculator__yearly-consumption"]'

        # submitbtn
        x_path_gas_submitbtn = '//*[@id="c84990"]/div/section/form/div[2]/div[3]/button'
        x_path_electricity_submitbtn = '//*[@id="c84995"]/div/section/form/div[2]/div[3]/button'

        # resultsite:
        x_path_gas_result_page = '//*[@id="c84990"]/div/div[2]/div/div/div[2]/div[1]/div[1]/div'
        x_path_electricity_result_page = '//*[@id="c84995"]/div/div[2]/div/div/div[2]/div[1]/div[1]/div'

        gas_consumption_value = 15000
        power_consumption_value = 3000

        gas_parameters = {'url': gas_url,
                        'regularity': x_path_gas_regularity,
                        'plz': x_path_gas_plz,
                        'plzerror': x_path_gas_plzerror,
                        'consumption': x_path_gas_consumption,
                        'submitbtn': x_path_gas_submitbtn,
                        'result_page': x_path_gas_result_page}

        electricity_parameters = {'url': electricity_url,
                                'regularity': x_path_electricity_regularity,
                                'plz': x_path_electricity_plz,
                                'plzerror': x_path_electricity_plzerror,
                                'consumption': x_path_electricity_consumption,
                                'submitbtn': x_path_electricity_submitbtn,
                                'result_page': x_path_electricity_result_page}

        if(self.energy_type == 'gas'):
            self.parameters = gas_parameters
        else:
            self.parameters = electricity_parameters

    def init_plz_data(self, plz_df):
        self.plz_df = plz_df

    def get_location_from_plzs(self):
        counter = 0
        driver = webdriver.Chrome('C:\chrome\chromedriver.exe', options=self.chrome_options)

        max_reties = 100
        retries = 0

        while self.plz_list:
            try:
                plzz = self.plz_list.popleft()
                counter += 1
                print('CURRENT PLZ: ',counter)
                result_sites = []
                places = []

                options = None
                driver = webdriver.Chrome('C:\chrome\chromedriver.exe', options=self.chrome_options)
                driver.implicitly_wait(15)
                driver.get(self.parameters['url'])
    
                time.sleep(2)

                if(counter == 1):
                    if(self.check_exists_by_xpath('//*[@id="cmpwelcomebtnyes"]/a', driver)):
                        print('accepting all cookies')
                        accept_cookies_btn = driver.find_element_by_xpath('//*[@id="cmpwelcomebtnyes"]/a')
                        accept_cookies_btn.click()

                plz = driver.find_element_by_xpath(self.parameters['plz'])                                       
                plz.send_keys(plzz)

                try:
                    plz_error =  WebDriverWait(driver, 1.5).until(
                                    EC.element_to_be_clickable((By.XPATH, self.parameters['plzerror']))
                                    )
                except:
                    plz_error = None

                if(plz_error != None):
                    print('PLZ invalid: ',plzz)
                    continue   

                #überprüfe ob xpath place existiert?
                options = driver.find_element_by_class_name('zipcode__city')
        

                d = Select(options)
                options = d.options

                for option in options:
                    print('hier ',option.get_attribute("value"))
 
                if(len(options) > 0):  
                    #places = [option.text for option in options]

                    places_id = [option.get_attribute("value") for option in options if len(option.get_attribute("value")) > 0]
                    places = [option.text.rsplit('/', 1)[-1].rstrip().lstrip() for option in options if len(option.get_attribute("value")) > 0]

                    time.sleep(1)
                else:
                    #print(options)
                    #places = [str(plzz)]
                    places_id = ['false']
                    places = [str(plzz)]
                    time.sleep(1)

                

                self.plz_places_id_dict[plzz] = {'location_id': places_id,
                                                 'location':places}
                #self.plz_places_dict[plzz]['location'] = places    
                
                #Seite ist ausgelesen nun packe alle tarife eines plz in df        
                driver.close()
                
            except: 
                retries +=1 

                if(retries < max_reties):
                    result_sites = []
                    places = []
                    print('error ', plzz)
                    self.plz_list.append(plzz)
                    continue
                else:
                    print('max retires reached')
                    
        #self.safe_data()
    
    def safe_data(self):
        now_ = datetime.now().strftime('%b %d %y %H_%M_%S')
        if(self.energy_type == 'electricity'):
            self.every_plz_df.to_csv('D:/ft_energy_data_analysis/data/raw/electricity/'+now_+'_electricity_1604.csv', index_label=False)
        elif(self.energy_type == 'gas'):
            self.every_plz_df.to_csv('D:/ft_energy_data_analysis/data/raw/gas/'+now_+'_gas_1604.csv', index_label=False)
        else:
            self.every_plz_df.to_csv('D:/ft_energy_data_analysis/data/raw/default/'+now_+'_default_1604.csv', index_label=False)

path = os.getcwd()
plzfile_path = os.path.abspath(os.path.join(path, os.pardir+'\data\/external'+'\/Postleitzahlen_und_Versorgungsgebiete Strom.xlsx'))
print(plzfile_path)
plz_df = pd.read_excel(plzfile_path, converters={'PLZ':str,'Stadt/Gemeinde':str,'Stadt/Gemeinde':str, 'Versorgungsgebiet':str, 'Grundversorger':str}) 
plzs = plz_df['PLZ'].to_list()
plz_to_location = PLZ_location_utility(energy_type='gas',plz_list=plzs)
plz_to_location.get_location_from_plzs()
print('Mit gas fertig!')         

#print(plz_to_location.plz_places_id_dict)

out_file = open('json_out_gas2','w+')
json.dump(plz_to_location.plz_places_id_dict,out_file, indent=2)




