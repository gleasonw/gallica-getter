from typing import AsyncGenerator, List, Optional
import aiohttp
from app.fetch import fetch_queries_concurrently
from pydantic import BaseModel
from lxml import etree


class PageQuery(BaseModel):
    ark: str
    page_num: int = 0
    grams_within: Optional[set[str]] = None

    @property
    def endpoint_url(self):
        return "https://gallica.bnf.fr/RequestDigitalElement"

    @property
    def params(self):
        return {
            "O": self.ark,
            "E": "ALTO",
            "Deb": self.page_num,
        }


class ConvertedXMLPage(BaseModel):
    page_num: int
    ark: str
    text: str
    grams_within: Optional[set[str]] = None


class PageText:
    """Wrapper for Gallica's RequestDigitalElement API, Gallica originally returns OCR in XML format for a document page. This class parses the XML to plain text for eventual JSON formatting."""

    @staticmethod
    async def get(
        page_queries: List[PageQuery],
        session: aiohttp.ClientSession,
    ) -> AsyncGenerator[ConvertedXMLPage, None]:
        for response in await fetch_queries_concurrently(
            queries=page_queries,
            session=session,
        ):
            if response is not None:
                try:
                    elements = etree.fromstring(
                        response.text, parser=etree.XMLParser(encoding="utf-8")
                    )
                except etree.XMLSyntaxError:
                    continue
                root = elements.find(".")
                name_space = root.attrib.get(
                    "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation"
                )
                if name_space is None:
                    continue
                name_space = name_space.split(" ")[0]
                layout = elements.find(f"{{{name_space}}}Layout")
                if layout is None:
                    continue
                page = layout.find(f"{{{name_space}}}Page")
                if page is None:
                    continue
                print_space = page.find(f"{{{name_space}}}PrintSpace")
                if print_space is None:
                    continue
                text_blocks = print_space.iterdescendants(f"{{{name_space}}}TextBlock")
                if text_blocks is None:
                    continue
                text: List[str] = []
                for text_block in text_blocks:
                    text_lines = text_block.findall(f"{{{name_space}}}TextLine")
                    if text_lines is None:
                        continue
                    for text_line in text_lines:
                        string_elements = text_line.findall(f"{{{name_space}}}String")
                        if string_elements is None:
                            continue
                        for string_element in string_elements:
                            text.append(string_element.attrib.get("CONTENT"))
                yield ConvertedXMLPage(
                    page_num=response.query.page_num,
                    ark=response.query.ark,
                    text=" ".join(text),
                    grams_within=response.query.grams_within or None,
                )
