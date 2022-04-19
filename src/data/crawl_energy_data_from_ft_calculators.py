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
from datetime import datetime

class FT_calculators_energy_data_crawler:
    def __init__(self, energy_type='electricity', consumption=3000, plz_list=[73033]):
        self.energy_type = energy_type
        self.init_parameters()
        self.consumption = consumption
        self.chrome_options = Options()
        self.chrome_options.headless = True
        self.every_plz_df = pd.DataFrame()
        self.plz_list = plz_list

    def init_parameters(self):
        gas_url = 'https://www.finanztip.de/gaspreisvergleich/'
        electricity_url = 'https://www.finanztip.de/stromvergleich/'

        # regularity:
        x_path_gas_regularity = '//*[@id="c84990"]/div/section/form/div[2]/div[2]/div[1]/div[3]/label'
        x_path_electricity_regularity = '//*[@id="c84995"]/div/section/form/div[2]/div[2]/div[1]/div[3]/label'

        # plz:
        x_path_gas_plz = '//*[@id="c84990"]/div/section/form/div[2]/div[1]/div[1]/div/div[2]/input'
        x_path_electricity_plz = '//*[@id="c84995"]/div/section/form/div[2]/div[1]/div[1]/div/div[2]/input'

        # plz error:
        x_path_gas_plzerror = '//*[@id="c84990"]/div/section/form/div[2]/div[1]/div[1]/div/div[2]/div'
        x_path_electricity_plzerror = '//*[@id="c84995]/div/section/form/div[2]/div[1]/div[1]/div/div[2]/div'

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

    def remove_tags(self, string):
        string = string.replace("\t", "")
        string = string.replace("\n", "")
        return string

    def get_int_from_aspect(self, aspect):
        if(aspect.__contains__('Woche')):
            aspect1 = aspect
            aspect = int(''.join(filter(str.isdigit, aspect)))*0.25
            #print('Ist in Wochen ',aspect,' ',aspect1)     
        elif(aspect.__contains__('Jahr')):
            aspect = int(''.join(filter(str.isdigit, aspect)))*12
        elif(aspect.__contains__('Jahr ')):
            aspect = 12
        else:
            aspect = int(''.join(filter(str.isdigit, aspect)))
        return aspect

    def convertStringToPrice(self, columnname, df):
        df[columnname] = df[columnname].str.replace('[^,0-9*]', '')
        df[columnname] = df[columnname].str.replace(',', '.')
        df[columnname] = pd.to_numeric(df[columnname])
            
    def check_exists_by_xpath(self, xpath, driver):
        try:
            driver.find_element_by_xpath(xpath)
        except NoSuchElementException:
            return False
        return True

    def save_result_pages_to_df(self, result_sites, places, plzz):

        comlete_df = pd.DataFrame()
    
        #speichere alle ergebnisse
        for resultsite, place in zip(result_sites, places):
            all_items   = resultsite.find_all('div',attrs={'class':"result-item"})
            #print('all items: ',len(all_items),' ',place,'  ',plzz)

            grundpreis = [self.remove_tags(item.find('span', string=[re.compile("^[0-9][\.]?[0-9]*,[0-9]*.*€/Jahr$")]).get_text()) for item in all_items]
            kwh_price = [self.remove_tags(item.find('span', string=[re.compile("^[0-9][\.]?[0-9]*,[0-9]*.*ct/kWh$")]).get_text()) for item in all_items]
            product_names = [self.remove_tags(item.find('div', attrs={'class':'color-text2 md:margin-bottom-m'}).get_text()) for item in all_items]
            prices        = [self.remove_tags(item.find('p',   attrs={'class':'condensed font-size-xxxl line-height-1'}).get_text()) for item in all_items]
            provider    = []

            for item in all_items: 
                
                if(item.find('img') != None):
                    provider.append(item.find('img').get('alt'))
                elif(item.find('p').get_text() != None):
                    provider.append(item.find('p').get_text())
                else:
                    'Weder Bild noch alternativ beschreibung'   

            #result_site = BeautifulSoup(driver.page_source, 'html.parser')
            # Define a dictionary containing Students data
            data = {'grundpreis': grundpreis,
                    'anbieter': provider,
                    'tarifname':   product_names,
                    'preis':     prices,
                    'kwh_price': kwh_price}

            # Convert the dictionary into DataFrame
            df = pd.DataFrame(data)

            for index, item in enumerate(all_items):  

                df.loc[index, 'plz']   = int(plzz)
                df.loc[index, 'place'] = place

                #Abschließbar bei/ Empfehlung
                if(item.find('button',attrs={'class':"fake color-border-white"}) != None):
                    if(item.find('button',attrs={'class':"fake color-border-white"}).get_text() == 'Nur beim Anbieter abschließbar'):
                        df.loc[index, 'abschliessbar_bei'] = 'Anbieter'
                    elif(item.find('button',attrs={'class':"fake color-border-white"}).get_text() == 'Keine Finanztip-Empfehlung'):
                        df.loc[index, 'abschliessbar_bei'] = 'KeineEmpfehlung'
                elif(item.find('img',attrs={'alt':"Verivox"}) != None):
                    df.loc[index, 'abschliessbar_bei'] = 'Verivox'
                elif(item.find('img',attrs={'alt':"Check 24"}) != None):
                    df.loc[index, 'abschliessbar_bei'] = 'Check24'

                if(len(item.find_all('li',attrs={'class':["pro", "con-red"]})) > 0) :
                    for pro in item.find_all('li',attrs={'class':["pro", "con-red"]}):
                        aspect = self.remove_tags(pro.get_text())

                        #Mindestvertragslaufzeit (in monate)
                        if(aspect.__contains__('Mindestvertragslaufzeit')):
                            df.loc[index, 'mindestlaufzeit_monate'] = self.get_int_from_aspect(aspect) 

                        #Preisgarantie (in monate)
                        if(aspect.__contains__('Preisgarantie')):
                            df.loc[index, 'preisgarantie_monate'] = self.get_int_from_aspect(aspect) 

                        #Kündigungsfrist (in wochen)
                        if(aspect.__contains__('Kündigungsfrist')):
                            if(aspect.__contains__('Jahr') | aspect.__contains__('Jahr ') | aspect.__contains__('Monat ') | aspect.__contains__('Monate')):
                                print(aspect)
                            else:
                                df.loc[index, 'Kuendigungsfrist'] = self.get_int_from_aspect(aspect)

                        #Vertragsverlängerung (in monaten)
                        if(aspect.__contains__('Vertragsverlängerung')):
                            if(aspect.__contains__('Woche')):
                                df.loc[index, 'vertragsverlaengerung_monate'] = int(''.join(filter(str.isdigit, aspect)))*0.25
                            elif(aspect.__contains__('Jahr')):
                                df.loc[index, 'vertragsverlaengerung_monate'] = int(''.join(filter(str.isdigit, aspect)))*12
                            else:
                                df.loc[index, 'vertragsverlaengerung_monate'] = int(''.join(filter(str.isdigit, aspect)))

                        #Keine Vorkasse (bool)
                        if(pro.attrs['class'][0] == 'con-red' and aspect.__contains__('Keine Vorkasse')):
                            df.loc[index, 'KeineVorkasse'] = False
                        else:
                            df.loc[index, 'KeineVorkasse'] = True

                        #Öko-Tarif
                        if(aspect.__contains__('Öko-Tarif')):
                            df.loc[index, 'Oeko'] = True
                        else:
                            df.loc[index, 'Oeko'] = False
                else:
                    print('Kein pros')

            comlete_df = pd.concat([df, comlete_df], axis=0)
            
        self.every_plz_df = pd.concat([comlete_df, self.every_plz_df], axis=0)

    def crawl_engergy_data(self):
        counter = 0
        driver = webdriver.Chrome('C:\chrome\chromedriver.exe', options=self.chrome_options)
        for plz_index, plzz in enumerate(self.plz_list):
            counter += 1
            print('CURRENT PLZ: ',counter)
            result_sites = []
            places = []

            options = None
            driver = webdriver.Chrome('C:\chrome\chromedriver.exe', options=self.chrome_options)
            driver.implicitly_wait(20)
            driver.get(self.parameters['url'])
            time.sleep(3)

            if(plz_index == 0):
                if(self.check_exists_by_xpath('//*[@id="cmpwelcomebtnyes"]/a', driver)):
                    print('accepting all cookies')
                    accept_cookies_btn = driver.find_element_by_xpath('//*[@id="cmpwelcomebtnyes"]/a')
                    accept_cookies_btn.click()

            regularity = driver.find_element_by_xpath(self.parameters['regularity'])
            regularity.click()

            plz = driver.find_element_by_xpath(self.parameters['plz'])                                       
            plz.send_keys(plzz)

            try:
                plz_error =  WebDriverWait(driver, 1.5).until(
                                EC.element_to_be_clickable((By.XPATH, self.parameters['plz_error']))
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

            if(len(options) > 0):  
                for i, option in enumerate(options):
                    if(i > 0):
                        places.append(option.text)
                        option.click()
                        time.sleep(3)

                        consumption = driver.find_element_by_xpath(self.parameters['consumption'])
                        consumption.clear()
                        consumption.send_keys(self.consumption)

                        submitbtn = driver.find_element_by_xpath(self.parameters['submitbtn'])
                        submitbtn.click()

                        wait_until_result_page = driver.find_element_by_xpath(self.parameters['result_page'])

                        result_site = BeautifulSoup(driver.page_source, 'html.parser')
                        result_sites.append(result_site)
            else:
                places.append(plzz)

                consumption = driver.find_element_by_xpath(self.parameters['consumption'])
                consumption.clear()
                consumption.send_keys(self.consumption)

                submitbtn = driver.find_element_by_xpath(self.parameters['submitbtn'])
                submitbtn.click()

                wait_until_result_page = driver.find_element_by_xpath(self.parameters['result_page'])

                result_site = BeautifulSoup(driver.page_source, 'html.parser')   
                result_sites.append(result_site)

            
            #Seite ist ausgelesen nun packe alle tarife eines plz in df        
            driver.close()
            self.save_result_pages_to_df(result_sites, places, plzz)
        self.safe_data()
    

    def safe_data(self):
        now_ = datetime.now().strftime('%b %d %y %H_%M_%S')
        if(self.energy_type == 'electricity'):
            self.every_plz_df.to_csv('D:/ft_energy_data_analysis/data/raw/electricity/'+now_+'_electricity_1604.csv', index_label=False)
        else:
            self.every_plz_df.to_csv('D:/ft_energy_data_analysis/data/raw/gas/'+now_+'_gas_1604.csv', index_label=False)


plz_df = pd.read_excel('D:/ft_energy_data_analysis/src/data/Postleitzahlen_und_Versorgungsgebiete Strom.xlsx', converters={'PLZ':str,'Stadt/Gemeinde':str,'Stadt/Gemeinde':str, 'Versorgungsgebiet':str, 'Grundversorger':str}) 
plzs = plz_df['PLZ'].to_list()
crawler = FT_calculators_energy_data_crawler(energy_type='gas',plz_list=plzs)
crawler.crawl_engergy_data()
print('Mit gas fertig!')                        
crawler2 = FT_calculators_energy_data_crawler(energy_type='eletricity',plz_list=plzs)
crawler2.crawl_engergy_data()
print('Mit strom fertig!')


