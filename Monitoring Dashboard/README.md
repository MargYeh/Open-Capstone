# Step 10 - Building a Monitor Dashboard

This part of the project creates a dashboard to monitor different metrics of the deployment. The dashboard is made through Grafana Cloud, and is connected to Databricks through Microsoft Azure Monitoring and Azure Log Analytics. 

The setup uses the following instructions along with the open source library Spark-monitor to gather metrics into Azure Log Analytics and Grafana Cloud.
Connecting Databricks to Azure Log Analytics: https://learn.microsoft.com/en-us/azure/architecture/databricks-monitoring/application-logs
Open Source Library used for monitoring: https://github.com/mspnp/spark-monitoring/tree/l4jv2
Connecting to Grafana: https://learn.microsoft.com/en-us/azure/architecture/databricks-monitoring/dashboards

## Setup
