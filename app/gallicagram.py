from datetime import datetime
from io import StringIO
import time
from typing import Dict, Literal, Optional

import aiohttp
from fastapi import HTTPException
import pandas as pd
from pydantic import BaseModel, validator


class GallicagramInput(BaseModel):
    term: str
    start_date: Optional[int] = 1789
    end_date: Optional[int] = 1950
    grouping: Literal["mois", "annee"] = "mois"
    source: Literal["livres", "presse", "lemonde"] = "presse"
    link_term: Optional[str] = None

    class Config:
        validate_assignment = True

    @validator("start_date")
    def set_start_date(cls, date):
        return date or 1789

    @validator("end_date")
    def set_end_date(cls, date):
        return date or 1950

    def getCacheKey(self):
        return f"{self.term}-{self.start_date}-{self.end_date}-{self.grouping}-{self.source}-{self.link_term}"


async def fetch_series(input: GallicagramInput):
    return await do_dataframe_fetch(
        "https://shiny.ens-paris-saclay.fr/guni/query",
        {
            "corpus": input.source,
            "mot": input.term.lower(),
            "from": input.start_date,
            "to": input.end_date,
        },
    )


async def fetch_series_linked_term(input: GallicagramInput):
    if input.link_term is None:
        print("No link term")
        return
    return await do_dataframe_fetch(
        "https://shiny.ens-paris-saclay.fr/guni/contain",
        {
            "corpus": input.source,
            "mot1": input.term.lower(),
            "mot2": input.link_term.lower(),
            "from": input.start_date,
            "to": input.end_date,
        },
    )


async def do_dataframe_fetch(url: str, params: Dict):
    print(f"Fetching {url} with params {params}")
    async with aiohttp.ClientSession() as session:
        start = time.time()
        async with session.get(url, params=params) as response:
            print(f"Fetched {response.url}")
            print(f"Took {time.time() - start} seconds")
            if response.status != 200:
                raise HTTPException(
                    status_code=503, detail="Could not connect to Gallicagram! Egads!"
                )
            return pd.read_csv(StringIO(await response.text()))


def transform_series(series_dataframe: pd.DataFrame, input: GallicagramInput):
    if input.grouping == "mois" and input.source != "livres":
        series_dataframe = (
            series_dataframe.groupby(["annee", "mois"])
            .agg({"n": "sum", "total": "sum"})
            .reset_index()
        )
    if input.grouping == "annee":
        series_dataframe = (
            series_dataframe.groupby(["annee"])
            .agg({"n": "sum", "total": "sum"})
            .reset_index()
        )

    def calc_ratio(row):
        if row.total == 0:
            return 0
        return row.n / row.total

    series_dataframe["ratio"] = series_dataframe.apply(
        lambda row: calc_ratio(row), axis=1
    )
    if all(series_dataframe.ratio == 0):
        raise HTTPException(status_code=404, detail="No occurrences of the term found")

    def get_unix_timestamp(row) -> float:
        year = int(row.get("annee", 0))
        month = int(row.get("mois", 1))

        dt = datetime(year, month, 1)
        return dt.timestamp() * 1000

    return series_dataframe.apply(
        lambda row: (get_unix_timestamp(row), row["ratio"]), axis=1
    ).tolist()
