from pyspark.sql.session import SparkSession
from library.logger import Log4j
from library.api_extract import NewsApiExtract
from library.ingest.ingest import IngestConfig
from library.config_utils import ConfigUtils
from library.utils import DictUtils
import os

SOURCE_NAME = "news_api"
DATASET = "top-headlines"

from delta.tables import *
from delta import configure_spark_with_delta_pip

spark = (
    SparkSession
    .builder
    .appName("NewsAPIProject")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.master", "local[3]"))

spark = configure_spark_with_delta_pip(spark, ["io.delta:delta-core_2.12:2.4.0", 
                                               "io.delta:delta-storage:2.4.0"]).getOrCreate()

spark.sparkContext.addPyFile("C:\\spark-3.4.4\\jars\\delta-storage-2.4.0.jar")

logger = Log4j(spark)
ingest_config = IngestConfig(**ConfigUtils.get_ingest_config(SOURCE_NAME)[DATASET])

logger.info("Retrieving data from the API")
rest_api_data = NewsApiExtract().get_data(DATASET)

logger.info("Saving the data to a df")
df = spark.createDataFrame(
    DictUtils.get_by_path(rest_api_data, ingest_config.json_data_path),
    IngestConfig.get_struct_schema(ingest_config.schema)
)
df.show(10)

# Ingest().overwrite_delta(df = df)

logger.info("Finishing the NewsProject")

spark.stop()