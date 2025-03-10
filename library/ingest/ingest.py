from pydantic import BaseModel
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import _parse_datatype_string, StructType
import yaml
import os

class Ingest:

     def overwrite_delta(self, df : DataFrame, output_path = 'landing_zone') -> None:
        """
        Write the dataframe to parquet files
        in the landing zone.
        """
        df.write \
            .option("overwriteSchema", "true") \
            .mode("overwrite") \
            .format("delta") \
            .save(output_path)
        
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