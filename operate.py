import json
from random import randint, seed
from datetime import datetime
from pymongo import MongoClient

import test_constants


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


def updates(file_name, coll_name, count, r):
    client = MongoClient(test_constants.test_uri)
    # read json in
    with open(file_name, 'rt') as f:
        all_lines = f.readlines()

        collection = client.get_database(test_constants.test_db).get_collection(coll_name)
        for i in range(0, count):
            s = all_lines[r[i]]
            doc = json.loads(s)
            print(f'updating: {doc}')
            collection.update_many({'name': doc['name'], 'age': doc['age']}, {'$set': {'modified': datetime.now()}})


def inserts(file_name, coll_name, count, r):
    client = MongoClient(test_constants.test_uri)
    # read json in
    with open(file_name, 'rt') as f:
        all_lines = f.readlines()

        collection = client.get_database(test_constants.test_db).get_collection(coll_name)
        for i in range(0, count):
            s = all_lines[r[i]]
            doc = json.loads(s)
            print(f'inserting new {doc}')
            doc['new'] = datetime.now()
            collection.insert_one(doc)


def deletes(file_name, coll_name, count, r):
    client = MongoClient(test_constants.test_uri)
    # read json in
    with open(file_name, 'rt') as f:
        all_lines = f.readlines()

        collection = client.get_database(test_constants.test_db).get_collection(coll_name)
        for i in range(0, count):
            s = all_lines[r[i]]
            doc = json.loads(s)
            print(f'deleting {doc}')
            doc['new'] = datetime.now()
            collection.delete_one({'name': doc['name'], 'age': doc['age']})


def run_all_operations(rand_list_size, ops_count):
    random_list = create_rand_index(rand_list_size)

    updates("one.json", "one", ops_count, random_list)
    inserts("one.json", "one", ops_count, random_list)
    deletes("one.json", "one", ops_count, random_list)


if __name__ == "__main__":
    run_all_operations(100000, 1000)
