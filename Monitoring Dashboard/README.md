# Step 10 - Building a Monitor Dashboard

This part of the project creates a dashboard to monitor different metrics of the deployment. The dashboard is made through Grafana Cloud, and is connected to Databricks through Microsoft Azure Monitoring and Azure Log Analytics. 

The setup uses the following instructions along with the open source library Spark-monitor to gather metrics into Azure Log Analytics and Grafana Cloud.
- Connect Databricks to Azure Log Analytics: https://learn.microsoft.com/en-us/azure/architecture/databricks-monitoring/application-logs
- Open Source Library used for monitoring: https://github.com/mspnp/spark-monitoring/tree/l4jv2
- Dashboard Creation with Grafana: https://learn.microsoft.com/en-us/azure/architecture/databricks-monitoring/dashboards

## Setup

### Databricks CLI
Begin with setting up Databricks CLI with 
```
winget search databricks
winget install Databricks.DatabricksCLI
```
and then run ```databricks configure```. You'll need to enter the Databricks Workspace URL and Personal Access Token here.

### Spark-monitoring
Follow the instructions at https://github.com/mspnp/spark-monitoring/tree/l4jv2

You'll need to collect the following information and enter it into **src/scripts/spark-monitoring.sh**: 
- Log analytics workspace ID
- Log analytics key
- AZ Subscription ID
- AZ Resource Group Name
- AZ Provider Namespace = Microsoft.Databricks
- AZ Resource Type = workspaces
- AZ Resource Name

The cluster in Databricks is set up with 15.4LTS (Spark 3.5, Scala 2.12). The Maven Profile that should be used is "dbr-15.4-lts".

Use Docker to create the jar with the following command (Using Windows)
```
docker run -it --rm -v %cd%:/spark-monitoring -v "%USERPROFILE%/.m2":/root/.m2 mcr.microsoft.com/java/maven:8-zulu-debian10 mvn -f /spark-monitoring/pom.xml clean install -P "dbr-15.4-lts"
```

Next, load in the jar to dbfs with the commands:
```
databricks fs mkdirs dbfs:/databricks/spark-monitoring
databricks fs cp --overwrite target/spark-monitoring_1.0.0.jar dbfs:/databricks/spark-monitoring/
```
After spark-monitoring.sh has been updated with the needed information, upload it to the Workspace
```
databricks workspace mkdirs /databricks/spark-monitoring
databricks workspace import --overwrite --format "AUTO" --file src/scripts/spark-monitoring.sh /databricks/spark-monitoring/spark-monitoring.sh
```

In the Cluster information, add in the following path to init scripts under Advanced Options

![image](https://github.com/user-attachments/assets/5316a577-b351-44dd-b516-945b85ab8d17)

Add the following path to Logging in case there is a need to debug problems

![image](https://github.com/user-attachments/assets/950a678f-00bf-4c6e-8ad2-9b84ab12e208)

After setup, the dataset needs to be reprocessed to obtain metrics. When this is done, Log Analytics can query data inside SparkListenerEvent_CL like shown:

![image](https://github.com/user-attachments/assets/35f72168-1c06-4c3a-a23d-072b89122bcb)





