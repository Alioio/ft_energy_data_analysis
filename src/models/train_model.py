import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime

class KMean_model_trainer:

    def __init__(self, featurespath='\data\/processed', modelpath='\models\/kmean', energy_type='gas'):
        path = os.getcwd()
        self.inputpath = os.path.abspath(os.path.join(path, os.pardir+featurespath+'\/'+energy_type))
        self.outputpath = os.path.abspath(os.path.join(path, os.pardir+modelpath+'\/'+energy_type))
        self.energy_type = energy_type
    
    def load_data(self, filename='mostrecent'):

        if(filename=='mostrecent'):
            list_of_files = glob.glob(self.inputpath+'/*.csv') 
            latest_file = max(list_of_files, key=os.path.getctime)
            data_filename = latest_file

            self.data = pd.read_csv(data_filename)

    


kmeans_model_trainer = KMean_model_trainer()
kmeans_model_trainer.load_data()
    
print(kmeans_model_trainer.data)