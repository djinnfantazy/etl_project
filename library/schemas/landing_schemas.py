from pyspark.sql.types import StructType, StructField, StringType

def landing_str_schema(col_name : str) -> StructType:
    """Return a schema with a single StringType field for JSON list transformation to a df"""
    landing_schema = StructType([StructField(col_name, StringType(), False)])
    return landing_schema