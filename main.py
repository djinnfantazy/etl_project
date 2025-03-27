from pyspark.sql.session import SparkSession
from pyspark.sql.functions import current_date
from library.logger import Log4j
from library.api_extract import NewsApiExtract
from library.ingest.ingest import IngestConfig, Ingest
from library.config_utils import ConfigUtils
from library.utils import DictUtils
import os

SOURCE_NAME = "news_api"
DATASET = "top-headlines"

from delta.tables import *
from delta import configure_spark_with_delta_pip

spark_builder = (
    SparkSession
    .builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.master", "local[3]")
    )

spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()

logger = Log4j(spark)
ingest_config = IngestConfig(**ConfigUtils.get_ingest_config(SOURCE_NAME)[DATASET])

logger.info("Retrieving data from the API")
rest_api_data = NewsApiExtract().get_data(DATASET)

logger.info("Saving the data to a df")
df = (
    spark.createDataFrame(
    DictUtils.get_by_path(rest_api_data, ingest_config.json_data_path),
    IngestConfig.get_struct_schema(ingest_config.schema))
    .transform(Ingest.add_metadata_column)
    )

Ingest().overwrite_delta(df = df, partitionBy="publishedAt")

logger.info("Finishing the NewsProject")

spark.stop()