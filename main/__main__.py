import json

import requests
from pymongo import MongoClient

from main.constants import DB_NAME
from main.html_util import HtmlUtil


def main():
    with open("config.json", mode="r") as config_file:
        config = json.load(config_file)

    db_username = config["db_username"]
    db_password = config["db_password"]
    db_host = config["db_host"]
    beer_collections: list[str] = config["beer_collections"]
    healthcheck_url = config["healthcheck_url"]

    uri = f"mongodb+srv://{db_username}:{db_password}@{db_host}/?retryWrites=true&w=majority"
    client = MongoClient(uri)
    db = client[DB_NAME]

    for collection_name in beer_collections:
        print(f"\nRefreshing all data for beers in collection {collection_name!r}")

        beers_collection = db[collection_name]

        util = HtmlUtil(beers_collection)

        beer_list = beers_collection.find()
        beer_ids_to_refresh = []
        for beer in beer_list:
            beer_ids_to_refresh.append(beer["id"])

        print(f"Refreshing {len(beer_ids_to_refresh)} existing beer documents to refresh\n")

        for beer_id in beer_ids_to_refresh:
            util.refresh_beer(beer_id)

    requests.get(healthcheck_url)
    print(f"Pinged {healthcheck_url}")


if __name__ == '__main__':
    main()
