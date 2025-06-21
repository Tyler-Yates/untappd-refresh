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

        print(f"Found {beers_collection.count_documents({})} existing beer documents\n")
        util = HtmlUtil(beers_collection)

        beer_list = beers_collection.find()
        for beer in beer_list:
            util.refresh_beer(beer["id"])

    requests.get(healthcheck_url)
    print(f"Pinged {healthcheck_url}")


if __name__ == '__main__':
    main()
