# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from crawl_energy_data_from_ft_calculators import FT_calculators_energy_data_crawler


class Data_preporsessor:

    def __init__(self, rawdatapath='\data\/raw', interimdatapath='\data\/interim', energy_type='gas'):
        path = os.getcwd()
        self.inputpath = os.path.abspath(os.path.join(path, os.pardir+rawdatapath+'\/'+energy_type))
        self.outputpath = os.path.abspath(os.path.join(path, os.pardir+interimdatapath+'\/'+energy_type))
        self.energy_type = energy_type

    def read_raw_data(self, filename='mostrecent'):

        if(filename=='mostrecent'):
            list_of_files = glob.glob(self.inputpath+'/*.csv') # * means all if need specific format then *.csv
            latest_file = max(list_of_files, key=os.path.getctime)
            rawdata_filename = latest_file

        self.rawdata = pd.read_csv(rawdata_filename, converters={'plz':float})
        self.rawdata = self.rawdata.sort_values(by='anbieter') 

    def format_string_to_price(self, columnname):
        self.rawdata[columnname] = self.rawdata[columnname].str.replace('[^,0-9*]', '', regex=True)
        self.rawdata[columnname] = self.rawdata[columnname].str.replace(',', '.', regex=True)
        self.rawdata[columnname] = pd.to_numeric(self.rawdata[columnname])
    
    def format_plz(self):
        self.rawdata.plz = self.rawdata.plz.astype(int)
        self.rawdata.plz = self.rawdata.plz.astype(str)

        for plz in self.rawdata['plz']:
            if(len(plz) == 4):
                self.rawdata.loc[self.rawdata['plz'] == plz, 'plz'] = str(0)+plz[:4]

    def format_data(self):
        self.format_string_to_price('preis')
        self.format_string_to_price('grundpreis')
        self.format_string_to_price('kwh_price')

        self.format_plz()

    def unique_contract_data(self):
        self.unique_contracts = pd.DataFrame()
        temp_rawdata = self.rawdata.copy()
        temp_rawdata['mean_kwh_price']      = self.rawdata.groupby(['anbieter', 'tarifname'])['kwh_price'].transform('median').round(decimals = 2)
        temp_rawdata['variance_kwh_price']  = self.rawdata.groupby(['anbieter', 'tarifname'])['kwh_price'].transform('var').round(decimals = 2)
        temp_rawdata['mean_grundpreis']     = self.rawdata.groupby(['anbieter', 'tarifname'])['grundpreis'].transform('median').round(decimals = 2)
        temp_rawdata['variance_grundpreis'] = self.rawdata.groupby(['anbieter', 'tarifname'])['grundpreis'].transform('var').round(decimals = 2)
        temp_rawdata['variance_grundpreis'] = self.rawdata.groupby(['anbieter', 'tarifname'])['grundpreis'].transform('var').round(decimals = 2)

        self.unique_contracts = temp_rawdata.drop_duplicates(subset=['anbieter', 'tarifname'])[['anbieter','tarifname', 'mean_kwh_price', 'mean_grundpreis',  'variance_kwh_price', 'variance_grundpreis', 'Oeko', 'mindestlaufzeit_monate', 'preisgarantie_monate', 'plz']]

    def safe_data(self):
        now_ = datetime.now().strftime('%b %d %y %H_%M_%S')

        self.rawdata.to_csv(os.path.join(self.outputpath,now_+'_'+self.energy_type+'.csv'), index_label=False)
        self.unique_contracts.to_csv(os.path.join(self.outputpath,'unique_contracts/'+now_+'_unique_contracts_'+self.energy_type+'.csv'), index_label=False)


ds_maker = Data_preporsessor()

ds_maker.read_raw_data()
ds_maker.format_data()
ds_maker.unique_contract_data()
ds_maker.safe_data()