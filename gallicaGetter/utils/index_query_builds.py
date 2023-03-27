from typing import List, Optional

import aiohttp
from gallicaGetter.fetch import fetch_queries_concurrently
from gallicaGetter.utils.parse_xml import get_num_records_from_gallica_xml
from gallicaGetter.queries import VolumeQuery
from gallicaGetter.queries import PaperQuery


async def build_indexed_queries(
    queries: List[VolumeQuery] | List[PaperQuery],
    session: aiohttp.ClientSession,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[VolumeQuery] | List[PaperQuery]:
    if limit:
        queries_with_num_results = []
        for query in queries:
            new_query = query.make_copy(start_index=offset or 0, num_records=limit)
            queries_with_num_results.append(new_query)
    else:
        queries_with_num_results = await get_num_results_for_queries(queries, session)
    return index_queries_by_num_results(
        queries_with_num_results, records_per_query=limit or 50
    )


async def get_num_results_for_queries(
    queries: List[VolumeQuery] | List[PaperQuery], session: aiohttp.ClientSession
) -> List[PaperQuery] | List[VolumeQuery]:
    responses = await fetch_queries_concurrently(queries, session)
    queries_with_num_results_state = []
    for response in responses:
        assert response.query is type(VolumeQuery) or type(PaperQuery)
        response.query.gallica_results_for_params = get_num_records_from_gallica_xml(
            response.text
        )
        queries_with_num_results_state.append(response.query)
    return queries_with_num_results_state


def index_queries_by_num_results(
    queries_num_results: List[PaperQuery] | List[VolumeQuery],
    records_per_query: int = 50,
) -> List[PaperQuery] | List[VolumeQuery]:
    if not queries_num_results:
        return []
    indexed_queries = []
    for query in queries_num_results:
        for i in range(0, query.gallica_results_for_params, records_per_query):
            indexed_queries.append(
                query.make_copy(start_index=i, num_records=records_per_query)
            )
    return indexed_queries
