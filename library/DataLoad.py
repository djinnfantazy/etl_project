from pyspark.sql import SparkSession, DataFrameReader
from pyspark.sql.types import *
from pyspark.pandas import *
import os
from pyspark.sql import functions as f
import requests

API_BASE = "https://newsapi.org/v2/top-headlines?country=us"
api_key = os.environ.get("NEWS_API_KEY")

def df_to_file(spark, df):
    spark = DataFrameReader(spark)
    df.write.mode("overwrite").json('landing_zone')

def api_resp_to_df(spark, sparkcontext):
    spark = SparkSession(spark)
    response = requests.get(API_BASE, headers={
            "X-Api-Key": api_key
        })
    data = response.json()
    articles = data['articles']
    rdd = sparkcontext.parallelize(articles)
    df = spark.createDataFrame(rdd)
    return df
    