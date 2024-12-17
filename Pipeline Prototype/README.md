# Pipeline Prototype

This part of the project expands on the previous ideas in Data Exploration to build a working prototype using a subset of the groups. The groups to be targeted are listed in config.ini.

The program is run through ```driver.py```. It will download the files corresponding to the groups listed in config.ini, clean them, and then save the finished dataframes locally as csv files. 

## How to run
- Enter your API key into configs.ini
- Pick the groups that will be examined in GROUPLIST
- Run driver.py

## Explanation
Driver.py uses collector.py and transformer.py to do the data collection and transforming. Error messages and metrics are logged into census_data.log. The results are outputted into the data folder as 

## Processing
Originally the data has many empty columns and rows with only the county/state/name filled out as seen below. I remove the empty columns, the empty rows, and any columns that are majorily filled with non-numerical data. Then jsons of the same groups are collected together and a corresponding year column is added. The resulting dataframe is outputted as a single csv file for each group.

Original: DP02 2009, (3221 row x 1196 column)

![image](https://github.com/user-attachments/assets/c28e1d84-6f89-4a5e-98a3-e894abb7f5f6)

Cleaned: DP02 2009, (3143 row x 576 column)

![image](https://github.com/user-attachments/assets/81ece27b-56b0-44eb-80cd-1e886102e175)

Final combined of DP0 2009-2022: (43997 row x 576 column)
The metrics can be seen in census_data.log
![image](https://github.com/user-attachments/assets/054397aa-5f19-48bb-8758-ece54c4a46f9)

