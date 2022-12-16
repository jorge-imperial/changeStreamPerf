import argparse
import json
from pprint import pprint

import pymongo
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
from pymongo.server_api import ServerApi

import test_constants


def populate(test_uri, test_db, test_files):
    client = MongoClient(test_uri) # , server_api=ServerApi('1'))

    client.drop_database(test_db)

    db = client.get_database(test_db)

    # import from json files.
    for (coll_name, file) in test_files:
        collection = db.get_collection(coll_name)
        collection.create_index([("name", pymongo.DESCENDING), ("age", pymongo.DESCENDING)], background=True)
        print(f'Importing file {file} into collection {coll_name}...')
        with open(file, 'rt') as f:
            requests = []
            for n, json_str in enumerate(f, start=1):
                doc = json.loads(json_str)
                requests.append(InsertOne(doc))

                if n % 10000 == 0:
                    try:
                        collection.bulk_write(requests)
                        print(f'flush {n}')
                    except BulkWriteError as bwe:
                        pprint(bwe.details)
                    requests = []


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='populate.py', description='Run operations in a Mongo Environment.')
    parser.add_argument('--uri', default=test_constants.test_uri)
    parser.add_argument('--db', default=test_constants.test_db)
    parser.add_argument('--files', default=test_constants.test_files)

    args = parser.parse_args()
    populate(args.uri, args.db, args.files)
