from pyspark.sql.session import SparkSession
from library.logger import Log4j
from library.data_transformation import NewsApiLandingTransform
from library.api_extract import NewsApiExtract
from library.ingest.ingest import Ingest, IngestConfig
from library.utils import DictUtils

SOURCE_NAME = "news_api"
DATASET = "top_headlines"

spark = (
    SparkSession
    .builder
    .config("spark.master", "local[3]")
    .getOrCreate()
)
logger = Log4j(spark)

# TODO: 
ingest_config = IngestConfig(**read_config(..., SOURCE_NAME)[DATASET])

logger.info("Retrieving data from the API")
rest_api_data = NewsApiExtract().get_data()

logger.info("Saving the data to a df")
df = spark.createDataFrame(
    DictUtils.get_by_path(rest_api_data, ingest_config.json_data_path),
    ingest_config.get_struct_schema(spark)
)
# landing_saver = NewsApiLandingTransform()
# articles_list = landing_saver.json_to_strings(articles)
# logger.info("Saving the data to a df")
# articles_df = landing_saver.resp_to_df(spark, articles_list, "articles")
# articles_df.show(n=20)
logger.info("Saving the dataframe to files")

Ingest().overwrite_parquet(articles_df)

logger.info("Finishing the NewsProject")

spark.stop()