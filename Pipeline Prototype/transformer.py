#%%

import json
import pandas as pd
import logging
import re
import os
import numpy as np

logger = logging.getLogger(__name__)
#%%
class Transformer:
    def __init__(self):
        self.dir = "data"
        self.dataframes = {}
    
    def get_group_year(self, file_name):
        """Gets the prefix and year from a file name."""
        match = re.match(r"(.*)_(\d{4})\.json$", file_name)
        if match:
            prefix = match.group(1)
            year = int(match.group(2))
            return prefix, year
        return None, None

    def json_to_cleaned_dataframe(self, file_path):
        """To be used on the first json of a prefix.
        Converts JSON file to dataframe, cleans it by removing null rows and columns and non-numerical data, 
        then adds a year column."""

        group, year = self.get_group_year(file_path)

        try:
            with open(file_path, 'r') as file:
                data = json.load(file)  
                
            columns_to_keep = ['GEO_ID', 'NAME', 'state', 'county']
            if isinstance(data, list):
                # Convert JSON to DataFrame
                df = pd.DataFrame(data[1:], columns=data[0])

                # Remove na columns
                df = df.dropna(axis=1, how='all')

                # remove na rows
                non_null_count = df.drop(columns=columns_to_keep).notna().sum(axis=1)
                df = df[non_null_count > 0]

                # remove columns not numeric
                df_numeric = df.drop(columns=columns_to_keep).apply(pd.to_numeric, errors='coerce')
                non_numeric_columns = df_numeric.columns[df_numeric.isna().any()].tolist()
                df = df.drop(columns=non_numeric_columns)

                # add the 'year' column at the end
                df['year'] = year

                #print(df.shape)

                return df
            else:
                logger.error(f"JSON structure has problems, {file_path}")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error reading JSON file: {e}")
            return pd.DataFrame()

    def combine(self, df, file_path):
        """Combines df with data in a new json. Also cleans the new data coming in from the json"""
        df_columns = df.columns.tolist()
        group, year = self.get_group_year(file_path)
        
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)

            if isinstance(data, list):
                # Convert JSON to DataFrame
                df2 = pd.DataFrame(data[1:], columns=data[0])
                #remove null rows
                non_null_count = df2.drop(columns=['GEO_ID', 'NAME', 'state', 'county']).notna().sum(axis=1)
                df2 = df2[non_null_count > 0]

                df2['year'] = year
                df2 = df2[df_columns]

                #print(df2.shape)
                df_combo = pd.concat([df, df2], axis=0, ignore_index=True)
                return df_combo
            else:
                logger.error(f"JSON structure has problems, {file_path}")
                return pd.DataFrame()
        
        except Exception as e:
            logger.error(f"Error reading JSON file: {e}")
            return pd.DataFrame()
        
    def process_all(self):
        """Loops through and groups the data files by prefix, then combines all the years of a given prefix"""
        files = [f for f in os.listdir(self.dir) if f.endswith(".json")]
        files.sort() 

        grouped_files = {}
        for file in files:
            prefix, year = self.get_group_year(file)
            if prefix:
                grouped_files.setdefault(prefix, []).append((year, file))

        # Process files for each prefix group
        for prefix, file_list in grouped_files.items():
            file_list.sort()  
            df = None

            for idx, (year, file_name) in enumerate(file_list):
                file_path = os.path.join(self.dir, file_name)
                if idx == 0:
                    df = self.json_to_cleaned_dataframe(file_path)
                else:
                    df = self.combine(df, file_path)

            if df is not None:
                self.dataframes[prefix] = df
                logger.info(f"Final combined DataFrame for {prefix}: {df.shape}")

if __name__ == "__main__":
    transformer = Transformer()
    transformer.process_all()

    #For saving to csv (change this later for other formats)
    for prefix, df in transformer.dataframes.items():
        output_file = os.path.join(transformer.dir, f"{prefix}_combined.csv")
        df.to_csv(output_file, index=False)
        logger.info(f"Saved combined DataFrame for {prefix} to {output_file}")

    #---------------
    # Combine files for DP02 and DP03
    # file_path = "data/acs_acs5_profile_DP02_2009.json"
    # df = transformer.json_to_cleaned_dataframe(file_path)
    # df_columns = df.columns.tolist()

    # file_path2 = "data/acs_acs5_profile_DP02_2010.json"
    # df2 = transformer.combine(df,file_path2)

    #%%
    #FOR DEBUGGING
    # file_path2 = "data/acs_acs5_profile_DP02_2010.json"
    # with open(file_path, 'r') as file:
    #     data = json.load(file)
    # df2 = pd.DataFrame(data[1:], columns=data[0])
    # non_null_count = df2.drop(columns=['GEO_ID', 'NAME', 'state', 'county']).notna().sum(axis=1)
    # df2 = df2[non_null_count > 0]
    # df2[df2['state'] == '72']

