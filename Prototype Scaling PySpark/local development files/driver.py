from logger_config import setup_logger
import logging
from collector import *
from transformer import *
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import functions as F

if __name__ == "__main__":
    #Setup logger
    setup_logger()

    logger = logging.getLogger(__name__)
    logger.info("Running main")

    #Find configs for collection, api key and list of groups to pick
    config_file = "configs.ini"
    config = get_configs(config_file)
    CENSUS_API_KEY = config["CENSUS_API_KEY"]
    GROUPLIST = config["GROUPLIST"]

    #Setup Spark
    spark = SparkSession.builder \
            .appName("DataTransformer") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6") \
            .getOrCreate()

    #Collecting the data
    collector = Collector(CENSUS_API_KEY, GROUPLIST, spark)

    # year = datetime.now().year #Use this instead if needing to run automatically for the current year
    year = 2022

    collector.getallupto(year)
    #collector.getyear(year) #Use this instead for getting data for a specific year

    #---- Transformer 
    transformer = Transformer(spark)
    transformer.process_all()

    #For saving to csv (change this later for other formats)
    for prefix, df in transformer.dataframes.items():
        output_file = f"{transformer.dir}/{prefix}_combined.parquet"
        df.write.parquet(output_file, mode="overwrite")
        logger.info(f"Saved combined DataFrame for {prefix} to {output_file}")



