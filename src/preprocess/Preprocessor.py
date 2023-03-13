import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from datetime import datetime
#import datetime
import os
import glob
import re


def find_project_root_path():
    # Traverse up the directory hierarchy until we find the directory containing README.md
    current_dir = os.getcwd()
    while True:
        if os.path.exists(os.path.join(current_dir, 'README.md')):
            return current_dir
        else:
            # Move up one directory level
            current_dir = os.path.dirname(current_dir)
            
PROJECT_ROOT_PATH= find_project_root_path()
DATA_SOURCE_PATH = os.path.join(PROJECT_ROOT_PATH, 'data', 'raw')
DATA_DESTINATION_PATH = os.path.join(PROJECT_ROOT_PATH, 'data', 'interim')


class Preprocessor:

    def unit_to_month(self, currentunit: str, value_with_current_unit: float) -> int:
        """
        Converts a value from its given unit to months.

        Parameters:
            currentunit (str): The unit of the given value (e.g. 'month', 'year', 'week', 'day', 'nan', 'once', 'indefinitely').
            value_with_current_unit (float): The value to be converted, along with its given unit.

        Returns:
            int: The converted value in months.

        Raises:
            None

        Example:
            If currentunit is 'year' and value_with_current_unit is 2.5, then the returned value will be 30.

        """
        if currentunit == 'month':
            return abs(value_with_current_unit)
        elif currentunit == 'year':
            return abs(value_with_current_unit * 12)
        elif currentunit == 'week':
            return abs(value_with_current_unit * 0.25)
        elif currentunit == 'day':
            return abs(value_with_current_unit / 30)
        elif currentunit in ['nan', 'once', 'indefinitely'] and value_with_current_unit == 0:
            return 0
        else:
            return -1

    def fill_missing_values(self, df: pd.DataFrame, columns: list, categorical_strategy: str, numerical_strategy: str) -> pd.DataFrame:
        """
        Fill missing values in the specified columns of a Pandas DataFrame.

        Parameters:
            df (pd.DataFrame): The DataFrame containing missing values to be filled.
            columns (list): A list of column names for which missing values should be filled.
            categorical_strategy (str): The method to use for filling missing categorical (non-numeric) values, such as 'most_frequent' or 'constant'.
            numerical_strategy (str): The method to use for filling missing numerical (numeric) values, such as 'mean' or 'median'.

        Returns:
            pd.DataFrame: The input DataFrame with missing values in the specified columns filled.

        Raises:
            None

        Example:
            If df is a DataFrame with missing values in columns 'A', 'B', and 'C', and categorical_strategy is 'most_frequent' and numerical_strategy is 'mean', 
            then the returned DataFrame will have the missing values in columns 'A', 'B', and 'C' filled using the most frequent value and mean value, respectively.

        """
        # Make a copy of the input DataFrame to avoid modifying it directly
        df_filled = df.copy()

        # Create separate lists of categorical and numerical columns to be filled
        categorical_columns = []
        numerical_columns = []
        for column in columns:
            if df[column].dtype == 'object':
                categorical_columns.append(column)
            else:
                numerical_columns.append(column)

        # Fill missing categorical values using the specified strategy
        if len(categorical_columns) > 0:
            categorical_imputer = SimpleImputer(strategy=categorical_strategy)
            df_filled[categorical_columns] = categorical_imputer.fit_transform(df_filled[categorical_columns])

        # Fill missing numerical values using the specified strategy
        if len(numerical_columns) > 0:
            numerical_imputer = SimpleImputer(strategy=numerical_strategy)
            df_filled[numerical_columns] = numerical_imputer.fit_transform(df_filled[numerical_columns])

        return df_filled

    def find_redundant_features(self, df: pd.DataFrame, threshold: float) -> list:
        """
        Find redundant features in a Pandas DataFrame by identifying columns with high correlation.

        Parameters:
            df (pd.DataFrame): The DataFrame containing the features to be checked for redundancy.
            threshold (float): The correlation threshold above which two features are considered highly correlated.

        Returns:
            list: A list of column names with redundant features.

        Raises:
            None

        Example:
            If df is a DataFrame with columns 'A', 'B', and 'C', where 'B' is highly correlated with 'A', and 'C' is not correlated with 
            either 'A' or 'B', then the returned list will be ['B'] because 'B' is redundant due to its high correlation with 'A'.

        """
        # Create a correlation matrix
        corr_matrix = df.corr().abs()

        # Find pairs of highly correlated features
        redundant_features = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i):
                if corr_matrix.iloc[i, j] > threshold:
                    colname = corr_matrix.columns[i]
                    if colname not in redundant_features:
                        redundant_features.append(colname)

        return redundant_features
    
    def convert_strings_to_floats(self, df: pd.DataFrame, cols: list) -> pd.DataFrame:
        """
        Convert strings to floats in specified columns of a Pandas DataFrame.

        Parameters:
            df (pd.DataFrame): The DataFrame to convert.
            cols (list): A list of column names to convert.

        Returns:
            pd.DataFrame: The converted DataFrame.

        Raises:
            None

        Example:
            If df is a DataFrame with columns 'A' and 'B', and cols=['A'], then the returned DataFrame will have column 'A' with string values converted to float values, replacing ',' with '.' as necessary.

        """
        for col in cols:
            df[col] = df[col].str.replace(',', '.').astype(float)

        return df
    
    def convert_to_bool(self, df: pd.DataFrame, cols: list) -> pd.DataFrame:
        """
        Convert specified columns of a Pandas DataFrame to boolean values.

        Parameters:
            df (pd.DataFrame): The DataFrame to convert.
            cols (list): A list of column names to convert.

        Returns:
            pd.DataFrame: The converted DataFrame.

        Raises:
            None

        Example:
            If df is a DataFrame with columns 'A' and 'B', and cols=['A'], then the returned DataFrame will have column 'A' with values converted to boolean values.

        """
        bool_dict = {0: False, 1: True, 'false': False, 'true': True, 'False': False, 'True': True}

        for col in cols:
            df[col] = df[col].replace(bool_dict).astype(bool)

        return df
    
    def add_date_and_consumption_columns(self, df: pd.DataFrame, filename: str) -> pd.DataFrame:
        """
        Add new  'date' and 'consumption' columns to a Pandas DataFrame based on information extracted from a filename.

        Parameters:
            df (pd.DataFrame): The DataFrame to add the columns to.
            filename (str): The filename containing the date and consumption information.

        Returns:
            pd.DataFrame: The DataFrame with the new columns added.

        Raises:
            None

        Example:
            If df is a DataFrame with columns 'A' and 'B', and filename='Jan 01 22_consumption.csv', then the returned DataFrame will have two new columns named 'data' and 'consumption', with values extracted from the filename.

        """
        date_str = filename.split('_')[0].split()[:-1]
        date_str = ' '.join(date_str)
        date = datetime.strptime(date_str, '%b %d %y')

        consumption = filename.split('_')[-1:][0].split('.')[0]

        df['date'] = date
        df['consumption'] = consumption

        return df


def load_energy_data(energy_type, consumption, data_source_dir):
    """
    Loads all CSV files in the specified directory that match the filename pattern "[15|3]+[0]{3}.csv",
    and returns a list of the filenames and a list of the corresponding dataframes.

    Args:
        energy_type (str): The type of energy to load (e.g. "gas", "electricity", etc.).
        consumption (str): The type of consumption to load (e.g. "high", "low", etc.).
        data_source_dir (str): The path to the root directory containing the CSV files.

    Returns:
        tuple: A tuple containing two lists - the list of filenames and the list of dataframes.
    """
    source_dir = os.path.join(data_source_dir, energy_type, consumption)
    filenames = []
    dataframes = []
    for file in os.listdir(source_dir):
        if re.search("(15000|3000|1300|9000).csv$", str(file)):
            filepath = os.path.join(source_dir, file)
            df = pd.read_csv(filepath,low_memory=False)
            filenames.append(file)
            dataframes.append(df)
    return filenames, dataframes

def load_new_raw_data_append_preporcessed():
    preprocessor = Preprocessor()
    energy_types = ['gas', 'electricity']
    consumptions = {'gas': ['9000', '15000'], 'electricity': ['1300', '3000']}

    for energy_type in energy_types:
        for consumption in consumptions[energy_type]:
            print(energy_type, consumption)

            # Read csv file from dir1 into a pandas DataFrame
            desitation_path = os.path.join(DATA_DESTINATION_PATH, energy_type, consumption)

            # specify the directory path
            directory_path = "/path/to/directory"

            # get a list of all files in the directory sorted by modification time
            list_of_files = glob.glob(desitation_path + "/*")
            sorted_list_of_files = sorted(list_of_files, key=os.path.getmtime)
            # get the filename of the newest file
            newest_file = sorted_list_of_files[-1]
            newest_filename = os.path.basename(newest_file)
            df1 = pd.read_csv(os.path.join(desitation_path, newest_filename),low_memory=False)

            # Read all csv files from dir2 into a list of pandas DataFrames
            dir2_path = os.path.join(DATA_SOURCE_PATH, energy_type, consumption)
            dates_df2 = []
            for file in os.listdir(dir2_path):
                if re.search("(15000|3000|1300|9000).csv$", str(file)):  
                    date_str = file.split('_')[0].split()[:-1]
                    date_str = ' '.join(date_str)
                    date = datetime.strptime(date_str, '%b %d %y')

                    if(pd.to_datetime(date) not in set(pd.to_datetime(df1['date']))):
                        print('missing date: ',date,'   ',os.path.join(dir2_path, file))
                        df2 = pd.read_csv(os.path.join(dir2_path, file),low_memory=False)
                        #df2['date'] = date

                        df2 = preprocessor.convert_to_bool(df2, ['dataeco', 'recommended'])
                        df2 = preprocessor.convert_strings_to_floats(df2, ['dataunit', 'datafixed'])
                        df2['kwh_price'] = ((df2['datafixed']*100)/3000) + df2['dataunit']
                        df2 = preprocessor.add_date_and_consumption_columns(df2, file)

                        if('commoncarrier' in df2.columns):
                            df2 = df2[['date', 'consumption','plz','location',  'providerName', 'tariffName', 'dataunit', 'datafixed', 'kwh_price','dataeco','datatotal','signupPartner','commoncarrier', 'priceGuaranteeNormalized', 'contractDurationNormalized']]
                        else:
                            df2 = df2[['date', 'consumption','plz','location',  'providerName', 'tariffName', 'dataunit', 'datafixed', 'kwh_price','dataeco','datatotal','signupPartner', 'priceGuaranteeNormalized', 'contractDurationNormalized']]
                        df1 = pd.concat([df1, df2], axis=0, ignore_index=True)
            df1.to_csv(os.path.join(desitation_path, newest_filename), index_label=False)


def load_all_raw_data_and_preproecess():
    preprocessor = Preprocessor()
    energy_types = ['gas', 'electricity']
    consumptions = {'gas': ['9000', '15000'], 'electricity': ['1300', '3000']}
    for energy_type in energy_types:
        for consumption in consumptions[energy_type]:
            print(energy_type, consumption)
            filenames, dfs = load_energy_data(energy_type, consumption, DATA_SOURCE_PATH)


            all_dfs = pd.DataFrame()
            for filename, df in zip(filenames, dfs):
                df = preprocessor.convert_to_bool(df, ['dataeco', 'recommended'])
                df = preprocessor.convert_strings_to_floats(df, ['dataunit', 'datafixed'])
                df['kwh_price'] = ((df['datafixed']*100)/3000) + df['dataunit']
                df = preprocessor.add_date_and_consumption_columns(df, filename)
                if('commoncarrier' in df.columns):
                    df = df[['date', 'consumption','plz','location',  'providerName', 'tariffName', 'dataunit', 'datafixed', 'kwh_price','dataeco','datatotal','signupPartner','commoncarrier', 'priceGuaranteeNormalized', 'contractDurationNormalized']]
                else:
                    df = df[['date', 'consumption','plz','location',  'providerName', 'tariffName', 'dataunit', 'datafixed', 'kwh_price','dataeco','datatotal','signupPartner', 'priceGuaranteeNormalized', 'contractDurationNormalized']]

                all_dfs = pd.concat([all_dfs, df], axis=0, ignore_index=True)

            now_ = datetime.now().strftime('%b %d %y %H_%M_%S')
            filename = now_+'_preprocessed_'+energy_type+'_'+consumption+'.csv'
            destination = os.path.join(DATA_DESTINATION_PATH, energy_type, consumption,filename)
            all_dfs.to_csv(destination, index_label=False)

#load_all_raw_data_and_preproecess()
#print(df1)

load_new_raw_data_append_preporcessed()


#print(df1)