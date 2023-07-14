from typing import List
import aiohttp
from pydantic import BaseModel
from gallicaGetter.fetch import post_queries_concurrently
from gallicaGetter.gallicaWrapper import GallicaWrapper


class ImageQuery(BaseModel):
    ark: str
    page: int
    term: str

    @property
    def payload(self):
        return {
          "ark": self.ark,
          "isPeriodique": True,
          "pages": [self.page],
          "query": f'(gallica any "{self.term}")',
          "limitSnippets": 1,
        }

    @property
    def endpoint_url(self):
        return "https://rapportgallica.bnf.fr/api/snippet"
    
    @property
    def headers(self):
        return {"Content-Type": "application/json"}


class ImageArgs(BaseModel):
    ark: str
    page: int
    term: str

class ImageSnippet(GallicaWrapper):
    """ Get images of occurrence paragraph/sentence from Gallica API. """

    def parse(self, gallica_responses):
        for response in gallica_responses:
            yield response
    
    async def get(
        self, 
        payloads: List[ImageArgs],
        session: aiohttp.ClientSession,
    ):
        queries = [
            ImageQuery(
                ark=payload.ark,
                page=payload.page,
                term=payload.term,
            )
            for payload in payloads
        ]
        return self.parse(
            await post_queries_concurrently(
                queries=queries,
                session=session,
            )
        )