from pyspark.sql import SparkSession, DataFrameReader
from abc import ABC, abstractmethod

class Ingest(ABC):

    @abstractmethod
    def df_to_file(self, df : DataFrameReader, output_path : str) -> None:
        pass
    
class NewsAPIIngest(Ingest):

    def df_to_file(self, df, output_path = 'landing_zone') -> None:
        df.write.mode("overwrite").json(output_path)