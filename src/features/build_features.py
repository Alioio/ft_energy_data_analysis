import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder, RobustScaler, MinMaxScaler
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import LabelEncoder, OrdinalEncoder
from sklearn.cluster import KMeans

class Feature_builder:

    def __init__(self, rawdatapath='\data\/interim', interimdatapath='\data\/processed', energy_type='gas'):
        path = os.getcwd()
        self.inputpath = os.path.abspath(os.path.join(path, os.pardir+rawdatapath+'\/'+energy_type))
        self.outputpath = os.path.abspath(os.path.join(path, os.pardir+interimdatapath+'\/'+energy_type))
        self.energy_type = energy_type

    def load_data(self, filename='mostrecent'):

        if(filename=='mostrecent'):
            list_of_files = glob.glob(self.inputpath+'/unique_contracts'+'/*.csv') # * means all if need specific format then *.csv
            latest_file = max(list_of_files, key=os.path.getctime)
            data_filename = latest_file

        self.data = pd.read_csv(data_filename)
        self.data = self.data.sort_values(by='anbieter') 
        self.data['Oeko'] = self.data['Oeko'].astype(int)

    def standardize(self):
        # numerical features from the dataset
        numerical_features = ['mean_grundpreis', 'mean_kwh_price']

        # categorical features from the dataset
        categorical_features = ['Oeko', 'mindestlaufzeit_monate', 'preisgarantie_monate', 'price_category']

        numerical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer()),
            ('scaler', 'passthrough')])

        categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value=0)),
            ('onehot', OrdinalEncoder())])

        data_transformer = ColumnTransformer(
            transformers=[
                ('numerical', numerical_transformer, numerical_features),
                ('categorical', categorical_transformer, categorical_features)])

        preprocessor = Pipeline(steps=[('data_transformer', data_transformer),
                             ('scaler',MinMaxScaler())])

        standardized_data = preprocessor.fit_transform(self.data)

        data = { 
        'grundpreis_scaled': standardized_data[:,0], 
        'kwh_price_scaled':standardized_data[:,1], 
        'Oeko_scaled':standardized_data[:,2],
        'mindestlaufzeit_monate_scaled':standardized_data[:,3],
        'preisgarantie_monate_scaled':standardized_data[:,4],
        'price_category':standardized_data[:,5],
        }

        self.standardized_data = pd.DataFrame(data)
        self.standardized_data = pd.concat([self.data[['anbieter', 'tarifname']].reset_index(drop=True), self.standardized_data.reset_index(drop=True)], axis=1)

    def safe_features(self):
        now_ = datetime.now().strftime('%b %d %y %H_%M_%S')

        self.standardized_data.to_csv(os.path.join(self.outputpath,now_+'_standardized_'+self.energy_type+'.csv'), index_label=False)

    def categorize_with_kmeans(self,columns=['mean_grundpreis', 'mean_kwh_price'], n_clusters =3):
        clusterer = KMeans(n_clusters=n_clusters, random_state=100)
        cluster_labels = clusterer.fit_predict(self.data[columns])
        self.data['price_category'] = cluster_labels



feature_builder = Feature_builder()
feature_builder.load_data()

print(feature_builder.data.head())

feature_builder.categorize_with_kmeans()
feature_builder.standardize()

print(feature_builder.standardized_data)
feature_builder.safe_features()