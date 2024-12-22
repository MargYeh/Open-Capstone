import requests
import os
import logging
import configparser
import numpy as np
from pyspark.sql import SparkSession, Row, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from functools import reduce
from dbutils import MockDbUtils
import sys

# logging.basicConfig(
#     filename="census_data.log",  # Log file name
#     level=logging.INFO,  # Logging level (INFO, DEBUG, WARNING, etc.)
#     format="%(asctime)s - %(levelname)s - %(message)s",
# )
logger = logging.getLogger(__name__)

if "dbutils" in sys.modules:
    pass  
else:
    # mock dbutils for local development
    from dbutils import MockDbUtils
    dbutils = MockDbUtils()

def get_configs(file_path):
    """Gets the configuration parameters from config.ini. Includes API key and a list of groups to download."""
    config = {}
    try:
        dbutils.fs.cp('dbfs:/FileStore/tables/configs.ini', "file:///tmp/configs.ini")
        config_parser = configparser.ConfigParser()
        config_parser.read(file_path)

        CENSUS_API_KEY = config_parser.get('settings', 'CENSUS_API_KEY')
        GROUPLIST = config_parser.get('settings', 'GROUPLIST')

        group_list = [tuple(group.split(',')) for group in GROUPLIST.splitlines() if group.strip()]

        config["CENSUS_API_KEY"] = CENSUS_API_KEY
        config["GROUPLIST"] = group_list

    except FileNotFoundError:
        logger.error(f"Config file '{file_path}' not found.")
        exit(1)
    except Exception as e:
        logger.error(f"Error reading config file: {e}")
        exit(1)

    return config



class Collector:
    def __init__(self, census_api_key, grouplist, spark_session):
        self.api_key = census_api_key
        self.grouplist = grouplist
        self.spark = spark_session

    def getdata(self, year, dataset, group):
        """Gets the data from the US Census API and downloads it to the data directory"""
        HOST = "http://api.census.gov/data"
        year = year
        dataset = dataset
        base_url = "/".join([HOST, year, dataset])
        predicates = {}

        census_api_key = self.api_key
        
        predicates["get"] = f"group({group})"
        predicates["for"] = "county:*"
        #predicates["in"] = "state:41,06,53"  # Limit to OR, CA, WA
        predicates["key"] = census_api_key

        try:
            r = requests.get(base_url, params=predicates)
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for year {year} and dataset {dataset}: {e}")
            return

        if r.status_code == 200:
            data = r.json()

        # First row is column and rest are rows
        if isinstance(data, list) and len(data) > 1:
            columns = data[0]  
            rows = data[1:]  

            # Create a list of dictionaries, where each dictionary corresponds to a row
            formatted_data = [dict(zip(columns, row)) for row in rows]

            #Clean it
            for row in formatted_data:
                for key, value in row.items():
                    if value is not None:
                        try:
                            row[key] = float(value)  # Convert to float if possible
                        except ValueError:
                            pass
            
            first_row = formatted_data[0]
            schema = StructType([
                StructField(key, DoubleType() if isinstance(value, (int, float)) else StringType(), True)
                for key, value in first_row.items()
            ])

            # Create an RDD from the formatted data
            rdd = spark.sparkContext.parallelize(formatted_data)

            # Create a DataFrame with the schema
            df_spark = spark.createDataFrame(rdd.map(lambda x: Row(**x)), schema=schema)

            # Specify the output directory and filename
            output_dir = "data"
            os.makedirs(output_dir, exist_ok=True)
            filename = f"{dataset.replace('/', '_')}_{group}_{year}.parquet"  # Replace slashes for valid filename
            file_path = os.path.join(output_dir, filename)

            # Save the DataFrame as a Parquet file
            df_spark.write.mode("overwrite").parquet(file_path)

            logger.info(f"Data successfully saved to {file_path}")

    def getyear(self, year):
        """Gets data for a specific year, for all listed groups"""
        for dataset, group in self.grouplist:
            self.getdata(str(year), dataset, group)
    
    def getallupto(self, year):
        """Gets all data up to a specific year, for all listed groups"""
        for year in range(2009, year+1):
            for dataset, group in self.grouplist:
                self.getdata(str(year), dataset, group)

if __name__ == "__main__":
    config_file = "configs.ini"
    config = get_configs(config_file)
    CENSUS_API_KEY = config["CENSUS_API_KEY"]
    GROUPLIST = config["GROUPLIST"]
    spark = SparkSession.builder.appName("Collector").getOrCreate()

    collector = Collector(CENSUS_API_KEY, GROUPLIST, spark)

    initial = input("Is this the initial run? Input Y to collect data for years 2005 to 2023")
    if initial.upper() == 'Y':
        collector.getallupto(2022)
        logger.info("Census data retrieval completed for all up to year {year} (All groups)")
    else:
        year = input("Enter single year")
        collector.getyear(year)
        logger.info("Census data retrieval completed for year {year} (All groups)")
