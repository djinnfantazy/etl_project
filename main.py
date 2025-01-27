from pyspark.sql import *
from pyspark import SparkConf
from library.logger import Log4j
from library.DataLoad import *


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
    
    raw_df = api_resp_to_df(spark, sparkcontext)

    logger.info("Saving the data to files")
    df_to_file(spark, raw_df)

    logger.info("Finishing the NewsProject")

    spark.stop()