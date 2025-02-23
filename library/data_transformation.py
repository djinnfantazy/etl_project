from pyspark.sql import SparkSession, DataFrame
from library.schemas.landing_schemas import landing_str_schema
import json

class NewsApiLandingTransform:

    @staticmethod
    def json_to_strings(json_obj : dict[str, str]) -> list[str]:
        """Tranfsorm the API JSON response to a list of strings"""
        list_of_str = []
        for item in json_obj:
            list_of_str.append(json.dumps(item))
        return list_of_str
    
    @staticmethod
    def resp_to_df(spark : SparkSession, api_resp : list, schema_col_name : str) -> DataFrame:
        """Transform the list of strings to a one-column dataframe"""
        rdd = spark.sparkContext.parallelize(api_resp)
        df = spark.createDataFrame(rdd.map(lambda x: (x,)), schema=landing_str_schema(schema_col_name))
        return df