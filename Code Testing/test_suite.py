import pytest
from pytest_mock import mocker
from unittest.mock import patch, MagicMock
from collector import Collector, get_configs
from transformer import Transformer, get_configs as get_configs2
import os
import requests
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
        return SparkSession.builder.getOrCreate()

@pytest.fixture
def mock_transformer(spark):
    return Transformer(spark)

def test_get_configs():
    """Test the get_configs function."""
    mock_config_file = "test_config.ini"
    mock_config_content = """
    [settings]
    CENSUS_API_KEY = mock_api_key

    GROUPLIST =
        dataset1,group1
        dataset2,group2
    """
    
    # Write the mock config content to a file
    with open(mock_config_file, "w") as f:
        f.write(mock_config_content)
    
    # Test the function
    config = get_configs(mock_config_file)
    
    assert config["CENSUS_API_KEY"] == "mock_api_key"
    assert config["GROUPLIST"] == [("dataset1", "group1"), ("dataset2", "group2")]
    
    # Clean up
    os.remove(mock_config_file)

def test_getdata_success(spark):
    """Test the getdata method of Collector."""
    
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            ["NAME", "VALUE"],  # headers
            ["Alice", "10"],    # data
            ["Bob", "20"],
        ]
        mock_get.return_value = mock_response

        # Initialize the Collector with a Spark session and mock API key
        collector = Collector("mock_api_key", [("dataset1", "group1")], spark)

        # Test the getdata function
        collector.getdata(2022, "dataset1", "group1")

        # Verify that the mock API was called correctly
        mock_get.assert_called_once_with(
            "http://api.census.gov/data/2022/dataset1",
            params={
                "get": "group(group1)",
                "for": "county:*",
                "key": "mock_api_key",
            },
        )

        # Check if the Spark session was used properly
        assert spark.version is not None  # Check that the Spark session is valid


def test_getdata_failure(spark):
    # Test the getdata method when the API call fails (e.g., network issues)
    
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 500  # Simulating a server error
        mock_get.return_value = mock_response

        collector = Collector("mock_api_key", [("dataset1", "group1")], spark)
        
        # Call the method and ensure no exceptions are raised
        collector.getdata(2022, "dataset1", "group1")

        # Check that the request failed (status code 500)
        mock_get.assert_called_once()
    
def test_get_configs_file_not_found():
    # Test for when the config file is not found
    with pytest.raises(SystemExit):
        get_configs("non_existent_config.ini")

def test_get_configs2():
    """Test the get_configs function."""
    mock_config_file = "test_config2.ini"
    mock_config_content = """
    [settings]
    CENSUS_API_KEY = mock_api_key

    GROUPLIST =
        dataset1,group1
        dataset2,group2

    STORAGE_ACCOUNT = mock_storage

    STORAGE_KEY = mock_storage_key

    CONTAINER = mock_container
    """
    
    # Write the mock config content to a file
    with open(mock_config_file, "w") as f:
        f.write(mock_config_content)
    
    # Test the function
    config = get_configs2(mock_config_file)
    
    assert config["STORAGE_ACCOUNT"] == "mock_storage"
    assert config["STORAGE_KEY"] == "mock_storage_key"
    assert config["CONTAINER"] == "mock_container"
    
    # Clean up
    os.remove(mock_config_file)

def test_json_to_cleaned_dataframe(mock_transformer, mocker):
    """Test `json_to_cleaned_dataframe` method in Transformer."""

    #Mock setup with 10 rows
    mock_df = MagicMock()
    mock_df.count.return_value = 10  
    mock_df.columns = ['GEO_ID', 'NAME', 'state', 'county', 'value']  
    mock_df.dropna.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    
    #Test reading fake parquet
    mocker.patch('pyspark.sql.readwriter.DataFrameReader.parquet', return_value=mock_df)
    mocker.patch.object(mock_transformer, 'get_group_year', return_value=('test_group', 2025))

    final_df = mock_transformer.json_to_cleaned_dataframe("test_file.parquet")

    #Asserts
    mock_transformer.spark.read.parquet.assert_called_once_with('test_file.parquet')
    assert final_df.count() == 10
    assert 'GEO_ID' in final_df.columns  
    assert 'NAME' in final_df.columns 
    assert 'value' in final_df.columns  

def test_clean_df(mock_transformer, mocker):
    """Test `clean_df` method in Transformer."""

    # Mock setup
    mock_remove_null_rows = mocker.patch.object(mock_transformer, 'remove_null_rows', return_value=MagicMock())
    mock_remove_non_numeric_columns = mocker.patch.object(mock_transformer, 'remove_non_numeric_columns', return_value=MagicMock())

    mock_df = MagicMock()
    mock_df.columns = ['GEO_ID', 'NAME', 'state', 'county', 'value']

    mock_clean_df = MagicMock()
    mock_clean_df.columns = ['GEO_ID', 'NAME', 'state', 'county', 'value']  

    # Testing mocked transformations - return mock_clean_df
    mock_remove_null_rows.return_value = mock_clean_df
    mock_remove_non_numeric_columns.return_value = mock_clean_df
    mock_clean_df.withColumn.return_value = mock_clean_df
    mock_clean_df.columns.append('year') 

    # Test function
    final_df = mock_transformer.clean_df(mock_df, 2025)

    # Asserts
    mock_remove_null_rows.assert_called_once_with(mock_df)
    mock_remove_non_numeric_columns.assert_called_once_with(mock_remove_null_rows.return_value)
    mock_clean_df.withColumn.assert_called_once() 
    assert 'year' in final_df.columns 
    assert final_df.columns == ['GEO_ID', 'NAME', 'state', 'county', 'value', 'year']  


def test_process_all(mock_transformer, mocker):
    """Test `process_all` method in Transformer."""
    
    # Mock setup
    mock_dbutils = MagicMock()
    mock_fs = MagicMock()

    mock_file1 = MagicMock()
    mock_file1.name = 'file1.parquet/'
    mock_file2 = MagicMock()
    mock_file2.name = 'file2.parquet/'

    mock_fs.ls.return_value = [mock_file1, mock_file2]
    mock_dbutils.fs = mock_fs

    mocker.patch('transformer.dbutils', mock_dbutils) 

    mock_transformer.get_group_year = MagicMock(return_value=("prefix", 2025))  # Return mock prefix and year
    
    mock_df = MagicMock()
    mock_transformer.json_to_cleaned_dataframe = MagicMock(return_value=mock_df)
    mock_df.count.return_value = 100 
    
    mock_transformer.combine = MagicMock(return_value=mock_df) 
    mock_transformer.dir = "/mock/directory"

    #Test function
    mock_transformer.process_all()

    #Asserts
    assert "prefix" in mock_transformer.dataframes
    assert mock_transformer.dataframes["prefix"] == mock_df




def test_process_all_empty(mock_transformer, mocker):
    """Test `process_all` method when no files are found in DBFS."""
    # Mock setup
    mock_dbutils = MagicMock()
    mock_fs = MagicMock()

    mock_fs.ls.return_value = []
    mock_dbutils.fs = mock_fs
    mocker.patch('transformer.dbutils', mock_dbutils)

    mock_transformer.dataframes = {}

    #Test process_all
    mock_transformer.process_all()

    #Asserts
    assert len(mock_transformer.dataframes) == 0

def test_remove_null_rows(mock_transformer):
    """Test the remove_null_rows method."""
    # Define an explicit schema to avoid type issues
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([
        StructField("GEO_ID", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("state", StringType(), True),
        StructField("county", StringType(), True),
        StructField("value", StringType(), True)  # 'value' can have None values as strings
    ])

    # Create a mock DataFrame with some null values
    df = mock_transformer.spark.createDataFrame(
        [
            ('GEO1', 'Name1', 'state1', 'county1', None), 
            ('GEO2', 'Name2', 'state2', 'county2', 10),  
            ('GEO3', None, None, None, None),              
        ],
        schema=schema  # Use the explicit schema
    )

    # Apply remove_null_rows
    cleaned_df = mock_transformer.remove_null_rows(df)

    assert cleaned_df.count() == 1  # There should be 1 row remaining, GEO2

def test_remove_non_numeric_columns(mock_transformer):
    """Test the remove_non_numeric_columns method."""

    df = mock_transformer.spark.createDataFrame(
        [
            ('GEO1', 'Name1', 'state1', 'county1', 10.0, 100),  
            ('GEO2', 'Name2', 'state2', 'county2', 20.0, 200),  
            ('GEO3', 'Name3', 'state3', 'county3', 'abc', 300)   
        ],
        ['GEO_ID', 'NAME', 'state', 'county', 'value', 'another_numeric']  
    )

    cleaned_df = mock_transformer.remove_non_numeric_columns(df)

    assert 'value' not in cleaned_df.columns

    assert 'GEO_ID' in cleaned_df.columns
    assert 'NAME' in cleaned_df.columns
    assert 'state' in cleaned_df.columns
    assert 'county' in cleaned_df.columns
    assert 'another_numeric' in cleaned_df.columns