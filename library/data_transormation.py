from pyspark.sql import SparkSession, DataFrameReader
from abc import ABC, abstractmethod

class LandingTransform(ABC):
    def __init__(self, 
                spark : SparkSession, 
                sparkcontext : SparkSession.sparkContext):
        
        self.spark = spark
        self.sparkcontext = sparkcontext
    
    @abstractmethod
    def resp_to_df(self, **kwargs) -> DataFrameReader:
        pass

class NewsApiLandingTransform(LandingTransform):

    def resp_to_df(self, api_resp) -> DataFrameReader:
        articles = api_resp['articles']
        rdd = self.sparkcontext.parallelize(articles)
        df = self.spark.createDataFrame(rdd)
        return df