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

        path = os.path.abspath(os.curdir)
        # path = os.path.dirname(__file__).split(cls.RESOURCES.value)[0].replace("\\", "/")
        return path


    def abs_path(self) -> str:
        """Get resources path"""

        return f"{self.root_path()}/{self.value}"


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

        full_path = f"{RootDir.RESOURCES.abs_path()}/{path}"

        return ConfigUtils.get_yaml_config(full_path)


    @staticmethod
    def get_ingest_config(ingest_dir: str) -> dict[str, Any]:
        """Get ingest config"""

        return ConfigUtils.get_resource_yaml_config(f"{ingest_dir}/ingest.yml")