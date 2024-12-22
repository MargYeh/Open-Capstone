# Open Capstone - Market Research - Step 6 - Pipeline Scaling with Pyspark

This step of the project refactored the previous Pipeline Prototype to work with Spark instead of Pandas. Spark is faster when working with large sets of data, making this a necessary step if working with big data. This part of the project was run on Databricks Community Edition, with the corresponding files located in the Databricks Working directory. Thoughts and a walkthrough of the process are shown in the Progress Slides PDF.

The notebooks in DataBricks Working will be uploaded in the future to Azure databricks. It uses Databrick's DBFS for storage instead of connecting to Azure Blob Storage. 

# Running and Examples
Run the collector.dbc to collect data. This downloads files as parquets to Databrick's DBFS
Run the transformer.dbc to clean the files, combine the cleaned files, and save them as {prefix}_combined in DBFS
The HTML files show the code and results from the notebook

# Results
Original DF that is downloaded by collector

![image](https://github.com/user-attachments/assets/7b986d40-1a1d-4223-82c5-944e1acfd564)
![image](https://github.com/user-attachments/assets/d737740d-2b11-47d1-ab27-c4490c7892a5)


After processing

![image](https://github.com/user-attachments/assets/82a3ed75-8aea-4798-89e1-48a5e95124ee)
![image](https://github.com/user-attachments/assets/ae05259a-7e66-4963-847f-86b48b88f120)



Final from DP02, processing years 2009-2011. Number of columns corresponds with results from the Pipeline Prototype

![image](https://github.com/user-attachments/assets/018f8047-c1d6-4c20-8324-b6e9e7d8f953)

