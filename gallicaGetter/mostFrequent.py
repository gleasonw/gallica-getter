from typing import Dict, List
from io import StringIO
import random
from collections import Counter
import aiohttp
from bs4 import BeautifulSoup
import os
from gallicaGetter.queries import VolumeQuery
from gallicaGetter.utils.index_query_builds import get_num_results_for_queries
from gallicaGetter.volumeOccurrence import VolumeOccurrence

from gallicaGetter.context import Context

here = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(here, "utils/stopwordsFR.txt"), "r") as stopwords_file:
    stopwords_fr = set(stopwords_file.read().splitlines())
with open(os.path.join(here, "utils/stopwordsEN.txt"), "r") as stopwords_file:
    stopwords_en = set(stopwords_file.read().splitlines())


async def get_gallica_core(
    root_gram: str,
    start_date: str,
    end_date: str,
    session: aiohttp.ClientSession,
    sample_size: int = 50,
) -> Dict[str, int]:
    """An experimental tool that returns the most frequent words in the surrounding context of a target word occurrence."""

    volume_wrapper = VolumeOccurrence()
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
    volumes_with_root_gram = await volume_wrapper.get(
        terms=[root_gram],
        start_date=start_date,
        end_date=end_date,
        start_index=indices_to_sample,
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
    notable_words_in_distance = get_associated_words(text_to_analyze, root_gram)
    return notable_words_in_distance


async def get_text_for_codes(
    codes: List[str], target_word: str, session: aiohttp.ClientSession
) -> str:
    text_wrapper = Context()
    text = ""
    text_records = await text_wrapper.get(
        [(code, [target_word]) for code in codes], session=session
    )
    for record in text_records:
        for page in record.pages:
            soup = BeautifulSoup(page.context, "html.parser")
            text += soup.get_text()
    return text


def get_associated_words(text_to_analyze: StringIO, root_gram: str) -> Dict[str, int]:
    counts = Counter(text_to_analyze.read().split())
    counts = {k.lower(): v for k, v in counts.items()}
    for stopword in stopwords_fr | stopwords_en:
        counts.pop(stopword, None)
    counts = {k: v for k, v in counts.items() if k.isalnum()}
    for word in root_gram.split():
        counts.pop(word, None)
    return counts
