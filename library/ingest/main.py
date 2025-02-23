from pyspark.sql import SparkSession
from library.logger import Log4j
from library.data_transformation import NewsApiLandingTransform
from library.api_extract import NewsApiExtract
from library.ingest.ingestion import Ingest

if __name__ == "__main__":
    spark = SparkSession.builder \
        .config("spark.app.name", "newsproject") \
        .config("spark.master", "local[3]") \
        .getOrCreate()
    
    logger = Log4j(spark)
    
    logger.info("Retrieving data from the API")
    
    newsapi = NewsApiExtract()
    articles = newsapi.get_data()
    landing_saver = NewsApiLandingTransform()
    articles_list = landing_saver.json_to_strings(articles)
    logger.info("Saving the data to a df")
    articles_df = landing_saver.resp_to_df(spark, articles_list, "articles")
    articles_df.show(n=20)
    logger.info("Saving the dataframe to files")
    landing_file = Ingest()
    landing_file.overwrite_parquet(articles_df)

    logger.info("Finishing the NewsProject")

    spark.stop()