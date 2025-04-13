from pydantic import BaseModel
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import _parse_datatype_string, StructType
from pyspark.sql.functions import current_date

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import current_date
from library.logger import Log4j
from library.api_extract import NewsApiExtract
from library.config_utils import ConfigUtils
from library.utils import DictUtils

from delta.tables import *
from delta import configure_spark_with_delta_pip
import os

class Ingest:
    
     @staticmethod
     def add_metadata_column(df: DataFrame) -> DataFrame:
        return df.withColumn("_createdOn", current_date())
     
     def overwrite_delta(self, df : DataFrame, partitionBy : str) -> None:
        
        output_path =  os.path.join(os.path.dirname(__file__), '..', 'data', 'bronze_layer')
        
        """
        Write the dataframe to parquet files
        in the bronze layer.
        """
        df.write \
            .option("overwriteSchema", "true") \
            .mode("overwrite") \
            .format("delta") \
            .partitionBy(partitionBy) \
            .save(output_path)

     def ingest_bronze(self, source_name : str, dataset : str):

        spark_builder = (
            SparkSession
            .builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.master", "local[3]")
            )

        spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()

        logger = Log4j(spark)
        ingest_config = IngestConfig(**ConfigUtils.get_ingest_config(source_name)[dataset])

        logger.info("Retrieving data from the API")
        rest_api_data = NewsApiExtract().get_data(dataset)

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

class IngestConfig(BaseModel):
    """ A class used to store config values """
    endpoint: str
    json_data_path: list[str] 
    schema: str

    @staticmethod
    def get_struct_schema(schema : str) -> StructType:
        ''' Parses a schema from the config file to be later used in dataframe creation'''
        schema_str = _parse_datatype_string(schema)
        return schema_str