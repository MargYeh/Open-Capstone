#%%

import json
import pandas as pd
import logging
import re
import os
import numpy as np
from pyspark.sql import SparkSession, Row, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, NumericType
from functools import reduce
import sys

logger = logging.getLogger(__name__)
#%%
class Transformer:
    def __init__(self, spark_session):
        self.dir = "dbfs:/data"
        self.spark = spark_session
        self.dataframes = {}

        global dbutils  # Declare dbutils as global to use directly in the class

        if "dbutils" in sys.modules:
            pass  
        else:
            # mock dbutils for local development
            from dbutils import MockDbUtils
            dbutils = MockDbUtils()
    
    def get_group_year(file_path):
        """Gets the prefix and year from a file name."""
        match = re.match(r"(.+?)_(\d{4})\.parquet$", file_path)
        if match:
            prefix = match.group(1)
            year = int(match.group(2))
            return prefix, year
        return None, None
    
    def drop_null_columns(df):
        cols_to_check = [col for col in df.columns]
        if len(cols_to_check) > 0:
            # drop columns for which the max is None
            rows_with_data = df.select(*cols_to_check).groupby().agg(*[F.max(c).alias(c) for c in cols_to_check]).take(1)[0]
            cols_to_drop = [c for c, const in rows_with_data.asDict().items() if const == None]
            new_df = df.drop(*cols_to_drop)

            return new_df
        else:
            return df
    
    def remove_null_rows(df):
        columns_to_keep = ['GEO_ID', 'NAME', 'state', 'county']
        new_df = df.dropna(how='all', subset=[col for col in df.columns if col not in columns_to_keep])
        return new_df
    
    def remove_non_numeric_columns(df):
        columns_to_keep = ['GEO_ID', 'NAME', 'state', 'county']

        numeric_columns = [
            col for col, dtype in df.dtypes
            if isinstance(df.schema[col].dataType, NumericType) and col not in columns_to_keep
        ]

        # Combine numeric columns and the columns to keep
        columns_to_retain = numeric_columns + columns_to_keep

        # Select only the columns to keep
        new_df = df.select(*columns_to_retain)

        return new_df

    
    def clean_df(self, df, year):
        df_no_null_columns = self.drop_null_columns(df)
        df_no_null_rows = self.remove_null_rows(df_no_null_columns)
        clean_df = self.remove_non_numeric_columns(df_no_null_rows)

        #add the year column
        final_df = clean_df.withColumn('year', F.lit(year))
        return final_df
    
    def json_to_cleaned_dataframe(self, file_path):
        """To be used on the first json of a prefix.
        Converts JSON file to dataframe, cleans it by removing null rows and columns and non-numerical data, 
        then adds a year column."""

        group, year = self.get_group_year(file_path)

        try:
            df_spark = self.spark.read.parquet(file_path)
            logger.info(f"Reading {file_path} into Spark DataFrame")
            logger.info(f"Columns: {len(df_spark.columns)}. Rows: {df_spark.count()}")
            
            final_df = self.clean_df(df_spark, year)
            logger.info(f"Finished cleaning. Columns: {len(final_df.columns)}. Rows: {final_df.count()}")
            
            return final_df

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            raise 

    def combine(self, df, file_path):
        """Reads incoming parquet file, cleans and then combines it with existing df. """
        df_columns = df.columns  # Get the columns from the first DataFrame
        group, year = self.get_group_year(file_path)  # Get the group and year from the file path

        try:
            # Read the Parquet file as a Spark DataFrame
            df2 = self.spark.read.parquet(file_path)

            # Clean the new DataFrame using the clean_df method
            df2_cleaned = self.clean_df(df2, year)

            # Keep only the columns from the first DataFrame
            df2_cleaned = df2_cleaned.select(*df_columns)

            # Union the cleaned DataFrame with the original one
            df_combined = df.union(df2_cleaned)

            return df_combined

        except Exception as e:
            logger.error(f"Error processing Parquet file {file_path}: {e}")
            return df  # Return the original DataFrame in case of error
        
    def process_all(self):
        """
        Loops through and groups the data files by prefix, then combines all the years 
        of a given prefix. Works with Databricks and Spark.
        """
        # List all files in the directory
        files = [f.name.rstrip('/') for f in dbutils.fs.ls(self.dir) if f.name.endswith(".parquet/")]
        files.sort()

        grouped_files = {}
        for file in files:
            prefix, year = self.get_group_year(file)
            if prefix:
                grouped_files.setdefault(prefix, []).append((year, file))

        # Process files for each prefix group
        for prefix, file_list in grouped_files.items():
            file_list.sort()  # Sort by year
            df = None

            for idx, (year, file_name) in enumerate(file_list):
                file_path = f"{self.dir}/{file_name}"  # Concatenate paths for Databricks
                if idx == 0:
                    # Convert the first file to a cleaned Spark DataFrame
                    df = self.json_to_cleaned_dataframe(file_path)
                else:
                    # Combine the current DataFrame with the new file
                    df = self.combine(df, file_path)

            if df is not None:
                self.dataframes[prefix] = df
                logger.info(
                    f"Final combined DataFrame for {prefix}: Rows: {df.count()}, Columns: {len(df.columns)}"
                )

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("DataTransformer") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6") \
            .getOrCreate()
    transformer = Transformer(spark)
    transformer.process_all()

    #For saving to csv (change this later for other formats)
    for prefix, df in transformer.dataframes.items():
        output_file = f"{transformer.dir}/{prefix}_combined.parquet"
        df.write.parquet(output_file, mode="overwrite")
        logger.info(f"Saved combined DataFrame for {prefix} to {output_file}")

   

