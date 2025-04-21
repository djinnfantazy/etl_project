from typing import Any
from enum import Enum
import os
import yaml

class RootDir(Enum):
    """Project directories"""

    RESOURCES = "resources"

    @classmethod
    def root_path(cls) -> str:
        """Get project root path"""

        path = os.path.join(os.path.dirname(__file__), '..')
        return os.path.abspath(path)


    def abs_path(self) -> str:
        """Get resources path"""
        abs_path = os.path.join(self.root_path(), self.value)
        return os.path.abspath(abs_path)


class ConfigUtils:
    """Config utils"""

    @staticmethod
    def get_yaml_config(path: str) -> dict:
        """Get yaml config"""

        with open(file=path, encoding="utf-8") as stream:
            config = yaml.safe_load(stream)

        return config

    @staticmethod
    def get_resource_yaml_config(path: str) -> dict[str, Any]:
        """Get resource yaml config"""

        full_path = os.path.join(RootDir.RESOURCES.abs_path(), path)

        return ConfigUtils.get_yaml_config(os.path.abspath(full_path))


    @staticmethod
    def get_ingest_config(ingest_dir: str) -> dict[str, Any]:
        """Get ingest config"""

        return ConfigUtils.get_resource_yaml_config(f"{ingest_dir}/ingest.yml")