from typing import Any, Callable

from fastapi import APIRouter
from fastapi.types import DecoratedCallable


class HandleTrailingSlashRouter(APIRouter):
    def add_api_route(
        self, path: str, endpoint: Callable[..., Any], *, include_in_schema: bool = True, **kwargs: Any
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        if path.endswith("/") and len(path) > 1:
            path = path[:-1]

        super().add_api_route(
            path, endpoint, include_in_schema=include_in_schema, **kwargs
        )

        alternate_path = path + "/"
        super().add_api_route(
            alternate_path, endpoint, include_in_schema=False, **kwargs
        )
