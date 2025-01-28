# Final Working Project for Deployment on Azure Blob and Azure Databricks

## Setup Instructions
The files contain the notebooks collector, transformer, and the config.ini that need to be uploaded into Databricks. config.ini should be uploaded directly as a file to DBFS at dbfs://FileStore/tables/. The collector and transformer can be uploaded into the Databricks Workspace.

### Azure Deployments
The project will need a Azure Blob Storage and Databricks deployment. The exact templates used for the deployment here are located in https://github.com/MargYeh/Open-Capstone/tree/main/Deployment%20Architecture%20Planning. 
A diagram of the architecture is shown below:

![image](https://github.com/user-attachments/assets/2542b11f-b43f-4500-9d15-ab99b501dde9)

### config.ini
After deploying the Azure Blob Storage, the following information in config.ini can be filled out. This file needs to be uploaded to the DBFS at dbfs://FileStore/tables/ inside of Azure Databricks.
- CENSUS_API_KEY: Before upload, change the information within config.ini to include the api key gained from https://api.census.gov/data/key_signup.html.
- GROUPLIST: Additional groups from https://censusreporter.org/topics/table-codes/ can be added to the list or existing groups can be commented out depending on what datasets are desired.
- STORAGE_ACCOUNT: Name of Azure Blob Storage used for persisting data
- STORAGE_KEY: Key of Azure Blob Storage
- CONTAINER: Container of Azure Blob Storage

