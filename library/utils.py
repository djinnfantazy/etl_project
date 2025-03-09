from typing import Any, Optional
import yaml
from pyspark.sql.types import _parse_datatype_string, StructType

class DictUtils:

    @staticmethod
    def read_config(config_path, endpoint : str):
        ''' Reads ingest configs for a particular endpoint  '''
        conf_dict = yaml.safe_load(open(config_path))[endpoint]
        return conf_dict

    @staticmethod
    def get_by_path(d: dict, path: Optional[list[str]] = None) -> Any:
        ''' Parses REST API's output by a json_path key in the YAML config file'''
        if path:
            subdict = d
            for key in path:
                subdict = subdict[key]
            return subdict

        return d
    
    @staticmethod
    def get_struct_schema(schema : str) -> StructType:
        ''' Parses a schema from the config file to be later used in dataframe creation'''
        schema_str = _parse_datatype_string(schema)
        return schema_str