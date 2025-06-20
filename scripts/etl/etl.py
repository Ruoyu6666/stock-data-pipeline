import yaml
import pandas as pd
import yfinance as yf

from pyspark.sql import SparkSession


# Load configuration from YAML file
with open("../../configs/etl.yaml") as f:
    etl_cfg = yaml.load(f, Loader=yaml.FullLoader)


def init_spark():
    '''
    Initialize a Spark session with PostgreSQL connector.
    Returns:
        SparkSession: Configured Spark session.
    '''
    spark = SparkSession.builder \
        .appName("Stock-Pipeline") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark




