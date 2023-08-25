import re
from typing import Dict, List
from io import StringIO
import random
import aiohttp
from bs4 import BeautifulSoup
import os
from gallicaGetter.queries import ContentQuery, VolumeQuery
from gallicaGetter.utils.index_query_builds import get_num_results_for_queries
from gallicaGetter.volumeOccurrence import VolumeOccurrence
from models import OccurrenceArgs

from gallicaGetter.context import Context

here = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(here, "utils/stopwordsFR.txt"), "r") as stopwords_file:
    stopwords_fr = set(stopwords_file.read().splitlines())
with open(os.path.join(here, "utils/stopwordsEN.txt"), "r") as stopwords_file:
    stopwords_en = set(stopwords_file.read().splitlines())


async def get_gallica_core(
    root_gram: str,
    start_date: str,
    max_n: int,
    session: aiohttp.ClientSession,
    end_date: str | None = None,
    sample_size: int = 50,
) -> Dict[str, int]:
    """An experimental tool that returns the most frequent words in the surrounding context of a target word occurrence."""

    num_volumes_with_root_gram = await get_num_results_for_queries(
        queries=[
            VolumeQuery(
                start_index=0,
                limit=1,
                terms=[root_gram],
                start_date=start_date,
                end_date=end_date,
            )
        ],
        session=session,
    )
    num_volumes = sum(
        query.gallica_results_for_params for query in num_volumes_with_root_gram
    )
    indices_to_sample = random.sample(range(num_volumes), sample_size)
    volumes_with_root_gram = await VolumeOccurrence.get(
        OccurrenceArgs(
            terms=[root_gram],
            start_date=start_date,
            end_date=end_date,
            start_index=indices_to_sample,
        ),
        session=session,
    )
    volume_codes = [
        volume_record.url.split("/")[-1] for volume_record in volumes_with_root_gram
    ]
    text_to_analyze = StringIO(
        await get_text_for_codes(
            codes=volume_codes, target_word=root_gram, session=session
        )
    )
    lower_text_string = text_to_analyze.read().lower()
    lower_text_array = re.findall(r"\b[a-zA-Z'àâéèêëîïôûùüç-]+\b", lower_text_string)
    root_grams = set(root_gram.split())
    filtered_array = []
    for i in range(len(lower_text_array)):
        word = lower_text_array[i]
        if word not in stopwords_fr | stopwords_en | root_grams:
            filtered_array.append(word)
    counts = {}
    for i in range(len(filtered_array)):
        for j in range(1, max_n + 1):
            if i + j <= len(filtered_array):
                word = " ".join(filtered_array[i : i + j])
                counts[word] = counts.get(word, 0) + 1
    return counts


async def get_text_for_codes(
    codes: List[str], target_word: str, session: aiohttp.ClientSession
) -> str:
    text = ""
    async for record in Context.get(
        queries=[ContentQuery(ark=code, terms=[target_word]) for code in codes],
        session=session,
    ):
        for page in record.pages:
            soup = BeautifulSoup(page.context, "html.parser")
            text += soup.get_text()
    return text
