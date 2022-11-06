# Testing change streams
from json import dumps

from pymongo import MongoClient
from pymongo.server_api import ServerApi

import test_constants


def watch():
    client = MongoClient(test_constants.test_uri, server_api=ServerApi('1'))
    print(f'Connected to "{test_constants.test_uri}"')

    db = client.get_database(test_constants.test_db)
    print(f'Watch database "{test_constants.test_db}"')

    change_stream = db.watch()
    for change in change_stream:
        print(change)
        print('')  # for readability only


if __name__ == '__main__':
    print('Watch collections')
    watch()
