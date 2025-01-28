import requests
import os
import logging
import configparser
import numpy as np
from pyspark.sql import SparkSession, Row, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from functools import reduce
import datetime
#from dbutils import MockDbUtils
import sys

logging.basicConfig(
    filename="collector.log",  # Log file name
    level=logging.INFO,  # Logging level (INFO, DEBUG, WARNING, etc.)
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

try:
    # Check if dbutils is available directly in the environment
    dbutils  # This checks if the dbutils is in the global namespace
    is_databricks = True
except NameError:
    # If dbutils is not available, it's likely not Databricks
    is_databricks = False

if not is_databricks:
    # Mock dbutils for local development (only for local testing)
    from dbutils import MockDbUtils
    dbutils = MockDbUtils()

def get_configs(file_path):
    """Gets the configuration parameters from config.ini."""
    config = {}
    try:
        # Check if the file exists
        logger.info(f"Reading config file from: {file_path}")
        
        if not os.path.exists(file_path):
            logger.error(f"Config file '{file_path}' does not exist.")
            dbutils.fs.cp('dbfs:/FileStore/tables/configs.ini', "file:///tmp/configs.ini")
            file_path = "/tmp/configs.ini"
        
        # Read the config file
        config_parser = configparser.ConfigParser()
        config_parser.read(file_path)
        
        # Debugging the sections and content of the file
        logger.info(f"Sections found: {config_parser.sections()}")
        
        # Check if the 'settings' section exists
        if 'settings' not in config_parser.sections():
            logger.error("No 'settings' section found in config file.")
            exit(1)
        
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
        base_url = "/".join([HOST, str(year), dataset])
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
        else:
            data = []

        # First row is column and rest are rows
        if len(data) > 1:
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
            rdd = self.spark.sparkContext.parallelize(formatted_data)

            # Create a DataFrame with the schema
            df_spark = self.spark.createDataFrame(rdd.map(lambda x: Row(**x)), schema=schema)

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
    print("This notebook is running as __main__")
    config_file = "configs.ini"
    config = get_configs(config_file)
    CENSUS_API_KEY = config["CENSUS_API_KEY"]
    GROUPLIST = config["GROUPLIST"]

    print(CENSUS_API_KEY)
    print(GROUPLIST)
    # spark = SparkSession.builder.appName("Collector").getOrCreate()

    # collector = Collector(CENSUS_API_KEY, GROUPLIST, spark)
    # year = 2011 #Change this if needed
    # collector.getallupto(int(year))

    #----
    #If collecting a single year, use this instead
    current_year = datetime.datetime.now().year
    #collector.getyear(int(current_year))

    # initial = input("Is this the initial run? Input Y to collect data for years 2009 to 2022")
    # if initial.upper() == 'Y':
    #     year = input("Enter year to get up to")
    #     collector.getallupto(int(year))
    #     logger.info("Census data retrieval completed for all up to year {year} (All groups)")
    # else:
    #     year = input("Enter single year")
    #     collector.getyear(int(year))
    #     logger.info("Census data retrieval completed for year {year} (All groups)")
