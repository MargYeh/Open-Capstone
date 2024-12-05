import requests
import json
import os
import pandas as pd
import logging

logging.basicConfig(
    filename="census_data.log",  # Log file name
    level=logging.INFO,  # Logging level (INFO, DEBUG, WARNING, etc.)
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()

def get_api_key(file_path):
    try:
        with open(file_path, "r") as file:
            return file.read().strip()
    except FileNotFoundError:
        logger.error(f"API key file '{file_path}' not found.")
        exit(1)
    except Exception as e:
        logger.error(f"Error reading API key file: {e}")
        exit(1)

API_KEY_FILE = "census_api_key.txt"
CENSUS_API_KEY = get_api_key(API_KEY_FILE)

def getdata(year, dataset, group):
    HOST = "http://api.census.gov/data"
    year = year
    dataset = dataset
    base_url = "/".join([HOST, year, dataset])
    predicates = {}

    api_key_file = "census_api_key.txt"
    census_api_key = get_api_key(api_key_file)
    
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

if __name__ == "__main__":
    initial = input("Is this the initial run? Input Y to collect data for years 2005 to 2023")
    if initial.upper() == 'Y':
        # for year in range(2009, 2022):  # Looping from 2009 to 2022
        #     getdata(str(year), "acs/acs5/profile", 'DP03')
        # logger.info("Census data retrieval completed for years 2009 to 2022, acs/acs5/profile, DP03")

        # for year in range(2009, 2022):  # Looping from 2009 to 2022
        #     getdata(str(year), "acs/acs5/profile", 'DP04')
        # logger.info("Census data retrieval completed for years 2009 to 2022, acs/acs5/profile, DP04")

        # for year in range(2009, 2022):  # Looping from 2009 to 2022
        #     getdata(str(year), "acs/acs5/subject", 'S0804')
        # logger.info("Census data retrieval completed for years 2009 to 2022, acs/acs5/subject, S0804")

        for year in range(2009, 2022):  # Looping from 2009 to 2022
            getdata(str(year), "acs/acs5/subject", 'S0804')
        logger.info("Census data retrieval completed for years 2009 to 2022, acs/acs5/subject, S0701")

        # for year in range(2009, 2022):  # Looping from 2009 to 2022
        #     getdata(str(year), "acs/acs5/subject", 'S2303')
        # logger.info("Census data retrieval completed for years 2009 to 2022, acs/acs5/subject, S2303")

        # for year in range(2009, 2022):  # Looping from 2009 to 2022
        #     getdata(str(year), "acs/acs5/cprofile", 'CP03')
        # logger.info("Census data retrieval completed for years 2009 to 2022, acs/acs5/cprofile, CP03")
    else:
        year = input("Enter single year")
        getdata(str(year), "acs/acs5/profile", 'DP03')
        getdata(str(year), "acs/acs5/profile", 'DP04')
        getdata(str(year), "acs/acs5/subject", 'S0804')
        getdata(str(year), "acs/acs5/subject", 'S2303')
        getdata(str(year), "acs/acs5/cprofile", 'CP03')
        logger.info("Census data retrieval completed for year {year}, acs/acs1, B19001")

