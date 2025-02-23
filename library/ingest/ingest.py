from pydantic import BaseModel
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import _parse_datatype_string, StructType


class Ingest:

    def overwrite_parquet(self, df : DataFrame, output_path = 'landing_zone') -> None:
        """
        Write the dataframe to parquet files
        in the landing zone.
        """
        df.write \
            .option("overwriteSchema", "true") \
            .mode("overwrite") \
            .parquet(output_path)
        

class IngestConfig(BaseModel):
    endpoint: str
    json_data_path: list[str]
    schema: str

    def get_struct_schema(self, spark: SparkSession) -> StructType:
        _parse_datatype_string(self.schema)