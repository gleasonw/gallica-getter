from dataclasses import dataclass
from typing import Any, Generator, List
from gallicaGetter.fetch import Response


class GallicaWrapper:
    """Base class for Gallica API wrappers."""

    def __init__(self):
        self.post_init()

    def get(self, **kwargs):
        raise NotImplementedError(
            f"get() not implemented for {self.__class__.__name__}"
        )

    def parse(
        self,
        gallica_responses: List[Response],
    ) -> Generator[Any, None, None]:
        raise NotImplementedError(
            f"parse() not implemented for {self.__class__.__name__}"
        )

    def post_init(self):
        pass
