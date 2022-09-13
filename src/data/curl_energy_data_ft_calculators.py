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
    def __init__(self, plz_locatin_dict, plz_df, energy_type='electricity', consumption=3000, bonus_include='1', maxContractProlongation='12', profile_id=0):
        self.profile_id = profile_id 
        self.energy_type = energy_type
        self.consumption = consumption
        self.bonus_include = bonus_include
        self.maxContractProlongation = maxContractProlongation
        self.every_plz_df = pd.DataFrame()
        self.plz_df = plz_df
        self.plz_location_dict = plz_locatin_dict
        self.plz_list = deque(self.plz_location_dict.keys())
        if(self.energy_type== 'electricity'):
            self.ENDPOINT = "https://www.finanztip.de/stromvergleich/powertool/?tx_ftcalculators_powertoolcalculator%5Baction%5D=result&tx_ftcalculators_powertoolcalculator%5Bcontroller%5D=PowerToolCalculator"
        else:
            self.ENDPOINT = "https://www.finanztip.de/gaspreisvergleich/gastool/?tx_ftcalculators_gastoolcalculator%5Baction%5D=result&tx_ftcalculators_gastoolcalculator%5Bcontroller%5D=GasToolCalculator"

    def safe_data(self):
        now_ = datetime.now().strftime('%b %d %y %H_%M_%S')
        if(self.energy_type == 'electricity'):
            self.every_plz_df.to_csv('D:/ft_energy_data_analysis/data/raw/electricity/'+now_+'_electricity_'+str(self.consumption)+'.csv', index_label=False)
        elif(self.energy_type == 'gas'):
            self.every_plz_df.to_csv('D:/ft_energy_data_analysis/data/raw/gas/'+now_+'_gas_'+str(self.consumption)+'.csv', index_label=False)
        else:
            self.every_plz_df.to_csv('D:/ft_energy_data_analysis/data/raw/default/'+now_+'_default_'+str(self.consumption)+'.csv', index_label=False)

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
                print('TRY WITH LOCATION')
                response = requests.request("POST", self.ENDPOINT, headers=headers, data=payload_location)
                print('Und danach: ',response['result']['recommendations'].keys())
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
                #print('new plz: ',plz)

                if(len(self.plz_df[self.plz_df['PLZ'] == plz]['Stadt/Gemeinde']) < 1):
                    print('STADT GEMEINDE NICTH RICHTIG: ',plz,'     !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')


                mainname = self.plz_df[self.plz_df['PLZ'] == plz]['Stadt/Gemeinde'].to_string(index=False)
                plzdf = pd.DataFrame()

                for location_id, location in zip(self.plz_location_dict[plz]['location_id'], self.plz_location_dict[plz]['location']):
                    #time.sleep(2)
                    print(location,'  ',location_id)
                    payload_mainname = "producttype=1&zipcode={zipcode}&mainname={mainname}&locationId={locationid}&annual={consumption}&bonusincluded=1&onlyEco=0&currFee=false&maxContractProlongation=12".format(zipcode = plz, mainname = mainname, locationid=location_id, consumption=self.consumption)
                    payload_location = "producttype=1&zipcode={zipcode}&mainname={location}&locationId={locationid}&annual={consumption}&bonusincluded=1&onlyEco=0&currFee=false&maxContractProlongation=12".format(zipcode = plz, location = location, locationid=location_id, consumption=self.consumption)

                    results_dict = self.post_request(payload_mainname, payload_location)

                    #print('new location: ',plz,'  ',location_id,' results dict: ',type(results_dict),' result: ',type(results_dict['result']),'  ',self.energy_type,'  ',mainname)
                    
                    plz_results_df = pd.DataFrame()

                    for recommendation_type in ['recommendations', 'nonRecommendations']:
                        #print('Das sind die Keys: ',results_dict['result'][recommendation_type])
                        
                        for resultobject in results_dict['result'][recommendation_type].keys():
                            #results_dict['result'][recommendation_type][resultobject]
                            #print('Das sind die Keys22222222: ',results_dict['result'][recommendation_type].keys())

                            if('tariffs' in results_dict['result'][recommendation_type][resultobject].keys()):
                                plz_results_df = plz_results_df.append(results_dict['result'][recommendation_type][resultobject]['tariffs'], ignore_index=True)
                                #print('plz:  ',plz,' location: ',mainname,'   ',' ro:',resultobject,'    ',results_dict['result'][recommendation_type][resultobject]['tariffs'])
                            else:
                                plz_results_df = plz_results_df.append(results_dict['result'][recommendation_type][resultobject], ignore_index=True)
                                #print('hier: ',mainname,'\n   ',results_dict['result'][recommendation_type][resultobject])

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
                    print('Here PAYLOAD: ',payload_mainname)
                    print('error ', plz,'  ',location,' exception: ',e,'   ',self.energy_type)
                    #print('Das sind die Keys: ',results_dict)
                    self.plz_list.append(plz)
                    continue
                else:
                    print('max retires reached')
                    
        self.safe_data()


# Opening JSON file
path = os.getcwd()
gas_location_dict_file = open('json_out_gas.json')
gas_location_json = json.load(gas_location_dict_file)
gas_location_dict = dict(gas_location_json)

electricity_location_dict_file = open('json_out_electricity.json')
electricity_location_json = json.load(electricity_location_dict_file)
electricity_location_dict = dict(electricity_location_json)


plzfile_path = os.path.abspath(os.path.join(path, os.pardir+'\data\/external'+'\/Postleitzahlen_und_Versorgungsgebiete Strom.xlsx'))
plz_df = pd.read_excel(plzfile_path, converters={'PLZ':str,'Stadt/Gemeinde':str,'Stadt/Gemeinde':str, 'Versorgungsgebiet':str, 'Grundversorger':str}) 

plz_df['Stadt/Gemeinde'] = plz_df['Stadt/Gemeinde'].str.replace('ü', 'ue')
plz_df['Stadt/Gemeinde'] = plz_df['Stadt/Gemeinde'].str.replace('Ü', 'Üe')
plz_df['Stadt/Gemeinde'] = plz_df['Stadt/Gemeinde'].str.replace('ö', 'oe')
plz_df['Stadt/Gemeinde'] = plz_df['Stadt/Gemeinde'].str.replace('Ö', 'Oe')
plz_df['Stadt/Gemeinde'] = plz_df['Stadt/Gemeinde'].str.replace('ä', 'ae')
plz_df['Stadt/Gemeinde'] = plz_df['Stadt/Gemeinde'].str.replace('Ä', 'Ae')
plz_df['Stadt/Gemeinde'] = plz_df['Stadt/Gemeinde'].str.replace('ß', 'ss')

gas_crawler_15000 = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=gas_location_dict, consumption=15000)
gas_crawler_5000 = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=gas_location_dict, consumption=9000)
#gas_crawler.crawl_engergy_data()

electricity_crawler_3000 = FT_calculators_energy_data_curl(energy_type='electricity', plz_df=plz_df, plz_locatin_dict=electricity_location_dict, consumption=3000)
electricity_crawler_1300 = FT_calculators_energy_data_curl(energy_type='electricity', plz_df=plz_df, plz_locatin_dict=electricity_location_dict, consumption=1300)
#electricity_crawler.crawl_engergy_data()

t1 = threading.Thread(target=gas_crawler_15000.crawl_engergy_data, name='t1')
t2 = threading.Thread(target=electricity_crawler_3000.crawl_engergy_data, name='t2')
t3 = threading.Thread(target=gas_crawler_5000.crawl_engergy_data, name='t3')
t4 = threading.Thread(target=electricity_crawler_1300.crawl_engergy_data, name='t4')   
  
# starting threads
t1.start()
#t2.start()
#t3.start()
#t4.start()

t1.join()
#t2.join()
#t3.join()
#t4.join()

print(gas_crawler_15000.every_plz_df)
#print(electricity_crawler_3000.every_plz_df)


'''
def iterate_profiles(): 

    for plz in ['10245', '99425', '33100', '50670', '49661']:
        gas_location_dict[plz]
        plz_dict = dict({plz:gas_location_dict[plz]})

        if(plz == '10245'):
            p1_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=5000, bonus_include='0', maxContractProlongation='1', profile_id=1)
            p1 = threading.Thread(target=p1_crawler.crawl_engergy_data, name='profil1')
            p2_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=5000, bonus_include='1', maxContractProlongation='12', profile_id=2)
            p2 = threading.Thread(target=p2_crawler.crawl_engergy_data, name='profil2')

            p3_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=15000, bonus_include='0', maxContractProlongation='1', profile_id=3)
            p3 = threading.Thread(target=p3_crawler.crawl_engergy_data, name='profil3')
            p4_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=15000, bonus_include='1', maxContractProlongation='12', profile_id=4)
            p4 = threading.Thread(target=p4_crawler.crawl_engergy_data, name='profil4')

            p1.start()
            p2.start()
            p3.start()
            p4.start()

        if(plz == '99425'):
            p5_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=5000, bonus_include='0', maxContractProlongation='1', profile_id=5)
            p5 = threading.Thread(target=p5_crawler.crawl_engergy_data, name='profil5')
            p6_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=5000, bonus_include='1', maxContractProlongation='12', profile_id=6)
            p6 = threading.Thread(target=p6_crawler.crawl_engergy_data, name='profil6')

            p7_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=15000, bonus_include='0', maxContractProlongation='1', profile_id=7)
            p7 = threading.Thread(target=p7_crawler.crawl_engergy_data, name='profil7')
            p8_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=15000, bonus_include='1', maxContractProlongation='12', profile_id=8)
            p8 = threading.Thread(target=p8_crawler.crawl_engergy_data, name='profil8')

            p5.start()
            p6.start()
            p7.start()
            p8.start()

        if(plz == '33100'):
            p9_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=5000, bonus_include='0', maxContractProlongation='1', profile_id=9)
            p9 = threading.Thread(target=p9_crawler.crawl_engergy_data, name='profil9')
            p10_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=5000, bonus_include='1', maxContractProlongation='12', profile_id=10)
            p10 = threading.Thread(target=p10_crawler.crawl_engergy_data, name='profil10')

            p11_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=15000, bonus_include='0', maxContractProlongation='1', profile_id=11)
            p11 = threading.Thread(target=p11_crawler.crawl_engergy_data, name='profil11')
            p12_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=15000, bonus_include='1', maxContractProlongation='12', profile_id=12)
            p12 = threading.Thread(target=p12_crawler.crawl_engergy_data, name='profil12')

            p9.start()
            p10.start()
            p11.start()
            p12.start()

        if(plz == '50670'):
            p13_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=5000, bonus_include='0', maxContractProlongation='1', profile_id=13)
            p13 = threading.Thread(target=p13_crawler.crawl_engergy_data, name='profil13')
            p14_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=5000, bonus_include='1', maxContractProlongation='12', profile_id=14)
            p14 = threading.Thread(target=p14_crawler.crawl_engergy_data, name='profil14')

            p15_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=15000, bonus_include='0', maxContractProlongation='1', profile_id=15)
            p15 = threading.Thread(target=p15_crawler.crawl_engergy_data, name='profil15')
            p16_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=15000, bonus_include='1', maxContractProlongation='12', profile_id=16)
            p16 = threading.Thread(target=p16_crawler.crawl_engergy_data, name='profil16')

            p13.start()
            p14.start()
            p15.start()
            p16.start()

        if(plz == '49661'):
            p17_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=5000, bonus_include='0', maxContractProlongation='1', profile_id=17)
            p17 = threading.Thread(target=p17_crawler.crawl_engergy_data, name='profil17')
            p18_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=5000, bonus_include='1', maxContractProlongation='12', profile_id=18)
            p18 = threading.Thread(target=p18_crawler.crawl_engergy_data, name='profil18')

            p19_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=15000, bonus_include='0', maxContractProlongation='1', profile_id=19)
            p19 = threading.Thread(target=p19_crawler.crawl_engergy_data, name='profil19')
            p20_crawler = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=plz_dict, consumption=15000, bonus_include='1', maxContractProlongation='12', profile_id=20)
            p20 = threading.Thread(target=p20_crawler.crawl_engergy_data, name='profil20')

            p17.start()
            p18.start()
            p19.start()
            p20.start()

   


gas_profil1 = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=gas_location_dict, consumption=15000)
gas_profil2 = FT_calculators_energy_data_curl(energy_type='gas', plz_df=plz_df, plz_locatin_dict=gas_location_dict, consumption=9000)
'''