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

You'll need to collect the following information:
- Log analytics workspace ID
- Log analytics key
- AZ Subscription ID
- AZ Resource Group Name
- AZ Provider Namespace = Microsoft.Databricks
- AZ Resource Type = workspaces
- AZ Resource Name

The cluster in Databricks is set up with 15.4LTS (Spark 3.5, Scala 2.12) 
