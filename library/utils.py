from typing import Any, Optional


class DictUtils:

    @staticmethod
    def get_by_path(d: dict, path: Optional[list[str]] = None) -> Any:

        if path:
            subdict = d
            for key in path:
                subdict = subdict[key]
            return subdict

        return d