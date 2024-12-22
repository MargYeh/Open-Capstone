import requests
import json
import os
import pandas as pd
import logging
import configparser

# logging.basicConfig(
#     filename="census_data.log",  # Log file name
#     level=logging.INFO,  # Logging level (INFO, DEBUG, WARNING, etc.)
#     format="%(asctime)s - %(levelname)s - %(message)s",
# )
logger = logging.getLogger(__name__)

def get_configs(file_path):
    """Gets the configuration parameters from config.ini. Includes API key and a list of groups to download."""
    config = {}
    try:
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
    def __init__(self, census_api_key, grouplist):
        self.api_key = census_api_key
        self.grouplist = grouplist

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
            r.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for year {year} and dataset {dataset}: {e}")
            if r.status_code == 404:
                logger.warning(f"Received 404 for year {year}, dataset {dataset}. It may not exist.")
            return

        #print(r.text)
        if r.status_code == 200:
            data = r.json()

            output_dir = "data"
            os.makedirs(output_dir, exist_ok=True) 

            filename = f"{dataset.replace('/', '_')}_{group}_{year}.json"  # Replace slashes for valid filename
            file_path = os.path.join(output_dir, filename)

            with open(file_path, "w") as json_file:
                json.dump(data, json_file, indent=4)  # Save with indentation for readability
            logger.info(f"Data successfully saved to {file_path}")
        else:
            logger.warning(f"Unexpected status code {r.status_code}: {r.text}")

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
    print(CENSUS_API_KEY)
    print(GROUPLIST)

    # collector = Collector(CENSUS_API_KEY, GROUPLIST)

    # initial = input("Is this the initial run? Input Y to collect data for years 2005 to 2023")
    # if initial.upper() == 'Y':
    #     collector.getallupto(2022)
    #     logger.info("Census data retrieval completed for all up to year {year} (All groups)")
    # else:
    #     year = input("Enter single year")
    #     collector.getyear(year)
    #     logger.info("Census data retrieval completed for year {year} (All groups)")

