# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

import dataclasses
import logging
import pickle
import re
import time
from datetime import date
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
from tqdm.auto import tqdm

from ..core.bases import FileMessage, Message
from ..core.source import Source

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class month_page:
    dt: date
    url: str


@dataclasses.dataclass
class SensorCommunitySource(Source):
    secrets_file: Path = Path("secrets.yaml")
    cache_directory: Path = Path("inputs/sensor.community")
    start_date: str = "2022-01-01"
    end_date: str = "2023-11-30"
    finish_after: int | None = None
    base_url: str = "https://archive.sensor.community/"

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)
        logger.debug(f"Initialialised SensorCommunity source with {self.start_date=}, {self.end_date=}")
        self.cache_file = self.resolve_path(self.cache_file, type="data")
        self.cache_directory = self.resolve_path(self.cache_directory, type="data")

    def load_cache_from_file(self):
        if not self.cache_file.exists():
            logger.info(f"Cache file doesn't exist, creating {self.cache_file}")
            self.cache_file.touch()

        with open(self.cache_file, "rb") as f:
            html_cache = {}
            try:
                while True:
                    k, v = pickle.load(f)
                    html_cache[k] = v
            except EOFError:
                pass

        logger.debug(
            f"Loaded html cache from disk, {len(html_cache)} entries,"
            f"{sum(len(v) for v in html_cache.values())/1e6:.0f} million characters"
        )
        self.html_cache = html_cache

    def cached_get_text(self, url):
        "request.get(url).text but aggressively cached"
        html_cache = self.html_cache
        if url not in html_cache:
            for i in range(80):
                try:
                    html_cache[url] = self.session.get(url).text
                    with open(self.cache_file, "ab") as f:
                        pickle.dump((url, html_cache[url]), f)
                    return html_cache[url]
                except RequestException:
                    print(
                        f"Connection failure {i}, backing off for {2**i} seconds",
                        end="\r",
                    )
                    time.sleep(2**i)
        return html_cache[url]

    def get_months_from_year(self, url):
        "Given a url for a particular year, get all the month folders"
        soup = BeautifulSoup(self.cached_get_text(url), "lxml")
        links = soup.find_all("a", dict(href=re.compile(r"^\d\d\d\d-\d\d-\d\d/$")))
        return [month_page(url=url + m["href"], dt=date.fromisoformat(m["href"][:-1])) for m in links]

    def get_months(self):
        "Get the data and url for every month for which sensor.community data exists"
        html = self.cached_get_text(self.base_url)
        soup = BeautifulSoup(html, "lxml")
        years = soup.find_all("a", dict(href=re.compile(r"^\d\d\d\d/$")))

        months = []
        for year in years:
            url = f"{self.base_url}{year['href']}"
            months.extend(self.get_months_from_year(url))

        # add the months from the current year that are shown on the main page
        months.extend(self.get_months_from_year(self.base_url))
        logger.debug(f"Sensor.Community: Found {len(months)} months")
        return sorted(months, key=lambda x: x.dt, reverse=True)

    def get_links_for_month(self, month):
        page = BeautifulSoup(self.cached_get_text(month.url), "lxml")
        return page.find_all("a", dict(href=re.compile(r"\.(csv)|(txt)$")))

    def generate(self) -> Iterable[Message]:
        emitted_messages = 0
        self.load_cache_from_file()
        self.session = requests.session()
        months = self.get_months()

        for month in tqdm(months[::1], desc="months", position=0):
            links = self.get_links_for_month(month)

            for link in links:
                url = (
                    month.url + link["href"]
                )  # e.g: https://archive.sensor.community/2023-06-19/2023-06-19_bme280_sensor_113.csv
                url_path = Path(urlparse(url).path)  # "/2023-06-19/2023-06-19_bme280_sensor_113.csv"
                filepath = Path(self.cache_directory) / f"{month.dt.year:04}" / month.dt.isoformat() / url_path.name

                if not filepath.exists():
                    logger.debug(f"Sensor.Community: Downloading {filepath}")
                    filepath.parent.mkdir(exist_ok=True, parents=True)
                    data = self.cached_get_text(url)
                    with open(filepath, "w") as f:
                        f.write(data)
                    time.sleep(0.1)

                yield FileMessage(
                    metadata=self.generate_metadata(
                        filepath=filepath,
                    ),
                )
                emitted_messages += 1
                if self.finish_after is not None and emitted_messages >= self.finish_after:
                    return
