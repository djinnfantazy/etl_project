from typing import Any, Optional
import yaml
from pyspark.sql.types import _parse_datatype_string, StructType

class DictUtils:

    @staticmethod
    def get_by_path(d: dict, path: Optional[list[str]] = None) -> Any:
        ''' Parses REST API's output by a json_path key in the YAML config file'''
        if path:
            subdict = d
            for key in path:
                subdict = subdict[key]
            return subdict

        return d
    