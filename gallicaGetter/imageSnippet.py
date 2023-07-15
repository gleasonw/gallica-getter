import json
from typing import List, Optional
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


class SnippetBean(BaseModel):
    content: Optional[str]


class PageData(BaseModel):
    pageCountResults: Optional[int]
    snippetBeans: Optional[List[SnippetBean]]

    @property
    def firstImage(self):
        if self.snippetBeans and len(self.snippetBeans) > 0:
            if self.snippetBeans[0].content:
                return self.snippetBeans[0].content
        return ""


class ImageResponse(BaseModel):
    ark: str
    page: int
    image: str
    term: str


class ImageSnippet(GallicaWrapper):
    """Get images of occurrence paragraph/sentence from Gallica API."""

    def parse(self, gallica_responses):
        for response in gallica_responses:
            json_response = json.loads(response.text)
            page_data = PageData(**json_response[0])
            yield ImageResponse(
                ark=response.query.ark,
                page=response.query.page,
                term=response.query.term,
                image=page_data.firstImage,
            )

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
