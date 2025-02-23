from pyspark.sql import DataFrame
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