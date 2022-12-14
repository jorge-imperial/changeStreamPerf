# Testing change streams
import argparse
from json import dumps

from pymongo import MongoClient
from pymongo.server_api import ServerApi

import test_constants


def watch(test_uri, test_db, output_to_file):
    client = MongoClient(test_uri) #, server_api=ServerApi('1'))
    print(f'Connected to "{test_uri}"')

    db = client.get_database(test_db)
    print(f'Watch database "{test_db}" and output to {output_to_file}')

    with open(output_to_file, 'wt') as output:
        change_stream = db.watch()
        for change in change_stream:
            output.write(str(change))
            output.write("\n")


if __name__ == '__main__':
    print('Watch collections')
    parser = argparse.ArgumentParser(prog='watch.py', description='Watch for changes in a Mongo Environment.')
    parser.add_argument('--uri', default=test_constants.test_uri)
    parser.add_argument('--db', default=test_constants.test_db)
    parser.add_argument('--out', default='watch.json')

    args = parser.parse_args()

    watch(args.uri, args.db, args.out)
