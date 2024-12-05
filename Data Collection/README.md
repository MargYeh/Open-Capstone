# Data Collection
This project uses data from the American Community Survey 5-Year Data (2009-2022), which is publically available at https://www.census.gov/data/developers/data-sets/acs-5year.html

## Targeted Groups
A list of all possible variables contained within the ACS is located here: https://censusreporter.org/topics/table-codes/
Of these codes, ```collection.py``` creates a json of each of the following at county level for each year from 2009-2022. Although the data is collected yearly, the ACS 5-year dataset pools data for each year with the previous five years. This results in a larger sample size with more accuracy. 

Group DP03 - Selected Economic Characteristics: Contains county level economic characteristics, income of various households, retirement, social security, health insurance
Group DP04 – Rent and Housing information
Group S0804 – Means of Transportation to Work by Selected Characteristics for Workplace Geography 
Group S0701 - Geographic mobility by selected characteristics in the United States
Group S2303 – Work status in the past 12 months
Group CP03 - Comparative Economic Characteristics

## How to run
Request an API key here: https://api.census.gov/data/key_signup.html and enter the API key into ```census_api_key.txt```

Run ```collection.py``` and input Y for the initial run.
The data will download as JSON files into the data directory, identified by dataset, group, and year.

![image](https://github.com/user-attachments/assets/3bc670f6-fc3b-4595-95f0-3e04d0e6907f)

It is also possible to download data for all groups for one specific year. This allows the script to run annually to update the dataset with data from future releases. 
