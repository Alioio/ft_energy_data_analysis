import time
import requests
import numpy as np
import pandas as pd
import json
import re
import os
from datetime import datetime
from collections import deque
import threading

class FT_calculators_energy_data_curl:
    def __init__(self, project_root, energy_type='electricity', consumption=3000, bonus_include='1', maxContractProlongation='12', profile_id=0):
        self.PORJECT_ROOT_PATH = project_root
        self.PORJECT_DESTINATION_PATH = os.path.join(PROJECT_ROOT_PATH, 'data', 'raw')
        self.PORJECT_SOURCE_PATH = os.path.join(PROJECT_ROOT_PATH, 'data', 'external')
        self.profile_id = profile_id 
        self.energy_type = energy_type
        self.consumption = consumption
        self.bonus_include = bonus_include
        self.maxContractProlongation = maxContractProlongation
        self.every_plz_df = pd.DataFrame()

        plz_df_filename = 'Postleitzahlen_und_Versorgungsgebiete Strom.xlsx'
        plzfile_path = os.path.join(self.PORJECT_SOURCE_PATH,'Postleitzahlen_und_Versorgungsgebiete Strom.xlsx')
        print('Source Paht: ',plzfile_path)
        plz_df = pd.read_excel(plzfile_path, converters={'PLZ':str,'Stadt/Gemeinde':str,'Stadt/Gemeinde':str, 'Versorgungsgebiet':str, 'Grundversorger':str}) 

        plz_df['Stadt/Gemeinde'] = plz_df['Stadt/Gemeinde'].str.replace('ü', 'ue')
        plz_df['Stadt/Gemeinde'] = plz_df['Stadt/Gemeinde'].str.replace('Ü', 'Üe')
        plz_df['Stadt/Gemeinde'] = plz_df['Stadt/Gemeinde'].str.replace('ö', 'oe')
        plz_df['Stadt/Gemeinde'] = plz_df['Stadt/Gemeinde'].str.replace('Ö', 'Oe')
        plz_df['Stadt/Gemeinde'] = plz_df['Stadt/Gemeinde'].str.replace('ä', 'ae')
        plz_df['Stadt/Gemeinde'] = plz_df['Stadt/Gemeinde'].str.replace('Ä', 'Ae')
        plz_df['Stadt/Gemeinde'] = plz_df['Stadt/Gemeinde'].str.replace('ß', 'ss')
        self.plz_df = plz_df

        json_dict_file_name = 'location_dict_'+self.energy_type+'.json'
        location_dict_file = open(os.path.join(self.PORJECT_SOURCE_PATH,'location_dicts',self.energy_type, json_dict_file_name))
        location_json = json.load(location_dict_file)
        location_dict = dict(location_json)
        self.plz_location_dict = location_dict

        self.plz_list = deque(self.plz_location_dict.keys())

        if(self.energy_type== 'electricity'):
            self.ENDPOINT = "https://www.finanztip.de/stromvergleich/powertool/?tx_ftcalculators_powertoolcalculator%5Baction%5D=result&tx_ftcalculators_powertoolcalculator%5Bcontroller%5D=PowerToolCalculator"
        else:
            self.ENDPOINT = "https://www.finanztip.de/gaspreisvergleich/gastool/?tx_ftcalculators_gastoolcalculator%5Baction%5D=result&tx_ftcalculators_gastoolcalculator%5Bcontroller%5D=GasToolCalculator"

    def safe_data(self):
        now_ = datetime.now().strftime('%b %d %y %H_%M_%S')
        filename = now_+'_'+self.energy_type+'_'+str(self.consumption)+'.csv'
        csv_path = os.path.join(self.PORJECT_DESTINATION_PATH, self.energy_type ,str(self.consumption) , filename)
        self.every_plz_df.to_csv(csv_path, index_label=False)
        
    def post_request(self, payload_mainname, payload_location):
        
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://www.finanztip.de',
            'Referer': 'https://www.finanztip.de/stromvergleich/'
        }

        response = None 
        try:
            response = requests.request("POST", self.ENDPOINT, headers=headers, data=payload_mainname)
            response =  response.json()
        except Exception as e:  
            print('call was not sucessfull: ',e)

        if(response['result']['recommendations'] == None):
            try:
                response = requests.request("POST", self.ENDPOINT, headers=headers, data=payload_location)
            except Exception as e:  
                print('call was not sucessfull: ',payload_location,'   exception: ',e)
        return response

    def crawl_engergy_data(self):
        print('here: ',self.energy_type)
        max_retries = 100
        retries = 0

        while self.plz_list:
            now_ = datetime.now().strftime('%b %d %y %H_%M_%S')
            try:
                plz = self.plz_list.popleft()

                if(len(self.plz_df[self.plz_df['PLZ'] == plz]['Stadt/Gemeinde']) < 1):
                    print('STADT GEMEINDE NICTH RICHTIG: ',plz,'     !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')


                mainname = self.plz_df[self.plz_df['PLZ'] == plz]['Stadt/Gemeinde'].to_string(index=False)
                plzdf = pd.DataFrame()

                for location_id, location in zip(self.plz_location_dict[plz]['location_id'], self.plz_location_dict[plz]['location']):
                    payload_mainname = "producttype=1&zipcode={zipcode}&mainname={mainname}&locationId={locationid}&annual={consumption}&bonusincluded=1&onlyEco=0&currFee=false&maxContractProlongation=12".format(zipcode = plz, mainname = mainname, locationid=location_id, consumption=self.consumption)
                    payload_location = "producttype=1&zipcode={zipcode}&mainname={location}&locationId={locationid}&annual={consumption}&bonusincluded=1&onlyEco=0&currFee=false&maxContractProlongation=12".format(zipcode = plz, location = location, locationid=location_id, consumption=self.consumption)

                    results_dict = self.post_request(payload_mainname, payload_location)
                    
                    plz_results_df = pd.DataFrame()


                    for recommendation_type in ['recommendations', 'nonRecommendations']:
                        
                        for resultobject in results_dict['result'][recommendation_type].keys():

                            if('tariffs' in results_dict['result'][recommendation_type][resultobject].keys()):
                                plz_results_df = pd.concat([plz_results_df, pd.DataFrame(results_dict['result'][recommendation_type][resultobject]['tariffs'])], ignore_index=True)

                            else:
                                plz_results_df = pd.concat([plz_results_df, pd.DataFrame([results_dict['result'][recommendation_type][resultobject]])], ignore_index=True)

                    plz_results_df['plz'] = str(plz)
                    plz_results_df['date'] = now_
                    plz_results_df['location_id'] = str(location_id)
                    plz_results_df['location'] = location           
                    plz_results_df['rank'] = plz_results_df.reset_index().index+1
                    plz_results_df['consumption'] = self.consumption
                    
                    #concat location df to plz df
                    plzdf = pd.concat([plzdf, plz_results_df], axis=0)

                #concat plz df to every plz df
                self.every_plz_df = pd.concat([self.every_plz_df , plzdf], axis=0)
            except Exception as e:
                retries +=1 

                if(retries < max_retries):
                    result_sites = []
                    places = []
                    print('error ', plz,'  ',location,' exception: ',e,'   ',self.energy_type)
                    self.plz_list.append(plz)
                    continue
                else:
                    print('max retires reached')
                    
        self.safe_data()


def find_project_root_path():
    current_dir = os.getcwd()
    # Traverse up the directory hierarchy until we find the directory containing README.md
    while True:
        if os.path.exists(os.path.join(current_dir, 'README.md')):
            return current_dir
        else:
            # Move up one directory level
            current_dir = os.path.dirname(current_dir)
            
PROJECT_ROOT_PATH = find_project_root_path()

gas_crawler_15000 = FT_calculators_energy_data_curl( project_root=PROJECT_ROOT_PATH, energy_type='gas', consumption=15000)
gas_crawler_5000 = FT_calculators_energy_data_curl(project_root=PROJECT_ROOT_PATH, energy_type='gas', consumption=9000)

electricity_crawler_3000 = FT_calculators_energy_data_curl(project_root=PROJECT_ROOT_PATH, energy_type='electricity',  consumption=3000)
electricity_crawler_1300 = FT_calculators_energy_data_curl(project_root=PROJECT_ROOT_PATH, energy_type='electricity',  consumption=1300)

t1 = threading.Thread(target=gas_crawler_15000.crawl_engergy_data, name='t1')
t2 = threading.Thread(target=electricity_crawler_3000.crawl_engergy_data, name='t2')
t3 = threading.Thread(target=gas_crawler_5000.crawl_engergy_data, name='t3')
t4 = threading.Thread(target=electricity_crawler_1300.crawl_engergy_data, name='t4')   
  
# starting threads
t1.start()
t2.start()
t3.start()
t4.start()

t1.join()
t2.join()
t3.join()
t4.join()

#print(gas_crawler_15000.every_plz_df)
print(electricity_crawler_3000.every_plz_df)