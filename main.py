from pyspark.sql import *
from pyspark import SparkConf
from library.logger import Log4j
# from library.data_load import api_resp_to_df, df_to_file
from library.data_transormation import NewsApiLandingTransform
from library.api_extract import NewsApiExtract
from library.ingestion import NewsAPIIngest
import os


os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.app.name", "newsproject")
    conf.set("spark.master", "local[3]")
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    
    sparkcontext = spark.sparkContext
    logger = Log4j(spark)
    
    logger.info("Retrieving data from the API")
    
    # raw_df = api_resp_to_df(spark, sparkcontext)
    newsapi = NewsApiExtract()
    articles = newsapi.extract()

    logger.info("Saving the data to a df")
    landing_saver = NewsApiLandingTransform(spark, sparkcontext)
    articles_df = landing_saver.resp_to_df(articles)

    logger.info("Saving the dataframe to files")
    landing_file = NewsAPIIngest()
    landing_file.df_to_file(articles_df)

    logger.info("Finishing the NewsProject")

    spark.stop()