import json
from random import randint, seed
from datetime import datetime
from pymongo import MongoClient

import test_constants

import argparse

""" 
Documents look like this:

     {
       "name":"Lawrence Bishop",
       "age":50,
       "created":{"$date":"2022-11-01T16:32:44.420Z"},
       "color":"white",
       "emails":["ha@bo.io","cah@gu.as","na@icisa.nr"]
     }
"""


def create_rand_index(rand_list_size):
    # A semi-random list of numbers that is repeateable
    seed(999)
    random_list = []
    for i in range(rand_list_size):
        random_list.append(randint(0, rand_list_size))
    return random_list


def updates(uri, file_name, coll_name, count, r, outf):
    client = MongoClient(uri)
    # read json in
    with open(file_name, 'rt') as f:
        all_lines = f.readlines()

        collection = client.get_database(test_constants.test_db).get_collection(coll_name)
        for i in range(0, count):
            s = all_lines[r[i]]
            doc = json.loads(s)
            doc['timestamp'] = datetime.now()
            if outf:
                outf.write(str(doc)+"\n")
            collection.update_many({'name': doc['name'], 'age': doc['age']}, {'$set': {'modified': datetime.now()}})


def inserts(uri, file_name, coll_name, count, r, outf):
    client = MongoClient(uri)
    # read json in
    with open(file_name, 'rt') as f:
        all_lines = f.readlines()

        collection = client.get_database(test_constants.test_db).get_collection(coll_name)
        for i in range(0, count):
            s = all_lines[r[i]]
            doc = json.loads(s)
            print(f'inserting new {doc}')
            doc['timestamp'] = datetime.now()
            if outf:
                outf.write(str(doc)+"\n")
            collection.insert_one(doc)


def deletes(uri, file_name, coll_name, count, r, outf):
    client = MongoClient(uri)
    # read json in
    with open(file_name, 'rt') as f:
        all_lines = f.readlines()

        collection = client.get_database(test_constants.test_db).get_collection(coll_name)
        for i in range(0, count):
            s = all_lines[r[i]]
            doc = json.loads(s)
            print(f'deleting {doc}')
            doc['timestamp'] = datetime.now()
            if outf:
                outf.write(str(doc)+"\n")
            collection.delete_one({'name': doc['name'], 'age': doc['age']})


def run_all_operations(uri, documents_file, coll_name, rand_list_size, ops_count,
                       output_update, output_insert, output_delete):
    random_list = create_rand_index(rand_list_size)
    print(f'Updating {uri} collection {coll_name}  {ops_count} operations')
    updates(uri, documents_file, coll_name, ops_count, random_list, output_update)
    print(f'Inserting {uri} collection {coll_name}  {ops_count} operations')
    inserts(uri, documents_file, coll_name, ops_count, random_list, output_insert)
    print(f'Deleting {uri} collection {coll_name}  {ops_count} operations')
    deletes(uri, documents_file, coll_name, ops_count, random_list, output_delete)

    print(f'Done with {uri} collection {coll_name}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='operate.py', description='Run operations in a Mongo Environment.')
    parser.add_argument('--uri', default=test_constants.test_uri)

    parser.add_argument('--out_update', type=argparse.FileType('w', encoding='UTF-8'))
    parser.add_argument('--out_insert', type=argparse.FileType('w', encoding='UTF-8'))
    parser.add_argument('--out_delete', type=argparse.FileType('w', encoding='UTF-8'))

    parser.add_argument('--list_size', default=400000)
    parser.add_argument('--operations', default=4000)
    args = parser.parse_args()

    run_all_operations(args.uri, documents_file='four.json', coll_name='four',
                       rand_list_size=args.list_size, ops_count=args.operations,
                       output_update=args.out_update,
                       output_insert=args.out_insert,
                       output_delete=args.out_delete)
