from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType
from abc import ABC, abstractmethod
import json

class NewsApiLandingTransform():
    def __init__(self, spark : SparkSession):
        self.spark = spark

    def json_to_strings(self, json_obj) -> list[str]:
        """
        Tranfsorm the API JSON response to a list of strings.
        """

        list_of_str = []
        for item in json_obj:
            list_of_str.append(json.loads(json.dumps(item)))
        return list_of_str

    def resp_to_df(self, spark : SparkSession, api_resp : list) -> DataFrame:
        """
        Transform the list of strings to a one-column dataframe.
        """
        schema = StructType().add("articles", StringType(), True)
        rdd = spark.sparkContext.parallelize(api_resp)
        df = spark.createDataFrame(rdd.map(lambda x: (x,)), schema=schema)
        return df