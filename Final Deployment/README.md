# Final Working Project for Deployment on Azure Blob and Azure Databricks
This project is a simple ETL setup that is designed to be deployed on the Azure cloud system. It gathers data from the American Community Survey 5-Year Data (2009-2022) on an automatic schedule, cleans and combines it, and then uploads the finished clean result as parquets and csv files to an Azure Blob Storage for further analysis (Using Tableau Public or other methods). 

## Setup Instructions

The files contain the notebooks collector, transformer, and the config.ini that need to be uploaded into Databricks. config.ini should be uploaded directly as a file to DBFS at dbfs://FileStore/tables/. The collector and transformer can be uploaded into the Databricks Workspace.

### Azure Deployments
The project will need a Azure Blob Storage and Databricks deployment. The exact templates used for the deployment here are located in https://github.com/MargYeh/Open-Capstone/tree/main/Deployment%20Architecture%20Planning. 

Databricks Setup: Used 15.4 LTS (Spark 3.5.0, Scala 2.12)

A diagram of the architecture is shown below:

![image](https://github.com/user-attachments/assets/2542b11f-b43f-4500-9d15-ab99b501dde9)

### config.ini
After deploying the Azure Blob Storage, the following information in config.ini can be filled out. This file needs to be uploaded to the DBFS at dbfs://FileStore/tables/ inside of Azure Databricks.
- CENSUS_API_KEY: Before upload, change the information within config.ini to include the api key gained from https://api.census.gov/data/key_signup.html.
- GROUPLIST: Additional groups from https://censusreporter.org/topics/table-codes/ can be added to the list or existing groups can be commented out depending on what datasets are desired.
- STORAGE_ACCOUNT: Name of Azure Blob Storage used for persisting data
- STORAGE_KEY: Key of Azure Blob Storage
- CONTAINER: Container of Azure Blob Storage

### Scheduling
Notebooks can be set to run on a yearly schedule through either the individual notebooks or from the Workflow section as individual jobs attached to notebooks. Set transformer to run after collector. Transformer will clean and combine all the same-named datasets from each year into one combined spark dataframe, and then save them as parquet and csv to the Azure Blob Storage information in config.ini. 

The first run of collector requires the following code to be uncommented to download all filesets into DBFS:

![image](https://github.com/user-attachments/assets/0bf19036-2ed6-4e47-96f4-5ef185fb0b79)

Afterwords, for subsequent runs comment out the previous code and uncomment this one instead:

![image](https://github.com/user-attachments/assets/4364ecbb-c6f1-4947-9b02-5b26080a580d)

This makes collector only check for additional data for the current year, if there is no new data then no change will happen.

## Results
Downloaded data after collector has run:

![image](https://github.com/user-attachments/assets/64694e48-27a9-41b4-85df-37903988a229)

Confirmation of data has been successfully collected:

![image](https://github.com/user-attachments/assets/aaf3a332-3640-40da-b817-067cab29fd88)

Confirmation of data downloaded to Azure Blob Storage after transformer run:

![image](https://github.com/user-attachments/assets/86baf7c8-3223-49d5-be13-c2004d662eed)

Sample of the metrics from the cluster used for the run:

![image](https://github.com/user-attachments/assets/0b147fc0-2843-47ed-a06b-fdef9e347c8f)

