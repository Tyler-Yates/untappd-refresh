import random
from time import sleep

import requests
from bs4 import BeautifulSoup
from pymongo import ASCENDING
from pymongo.collection import Collection

from main.constants import BEER_PAGE_FORMAT, REQUEST_HEADERS


class HtmlUtil:
    def __init__(self, beers_collection: Collection):
        self.beers_collection = beers_collection
        self.beers_collection.create_index([('id', ASCENDING)], unique=True, background=True)

        self.beers_already_processed = set()

    def refresh_beer(self, beer_id: int):
        # We could have the same beer across multiple collections, and we only want to refresh each beer once.
        if beer_id in self.beers_already_processed:
            return

        url = BEER_PAGE_FORMAT % beer_id
        response = requests.get(url, headers=REQUEST_HEADERS)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html5lib')

        # Get the beer identity
        beer_name = soup.find(class_="name").find("h1").get_text().strip()

        # These are the main values that could be changed over time
        style = soup.find(class_="style").get_text().strip()

        try:
            abv = float(soup.find(class_="abv").get_text().strip().rstrip("% ABV"))
        except ValueError:
            abv = -1

        # Only update the fields we fetched
        fields_to_update = {
            "style": style,
            "abv": abv,
        }
        update_result = self.beers_collection.update_one({"id": beer_id}, {"$set": fields_to_update}, upsert=False)

        print(f"Refreshing beer {beer_name!r} with id {beer_id} ...")
        print(f"Update result: Matched {update_result.matched_count} document(s) and updated {update_result.modified_count} document(s)")

        self.beers_already_processed.add(beer_id)

        # We don't want to overload Untappd with too many requests, so sleep a bit after we process a beer
        sleep_time = random.uniform(3, 30)
        print(f"Sleeping {sleep_time:.2f} seconds...")
        sleep(sleep_time)
        print("")
