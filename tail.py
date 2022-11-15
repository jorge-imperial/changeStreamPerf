import datetime
import time
from datetime import datetime
import argparse

from bson import Timestamp
from pymongo import MongoClient, ReadPreference, ASCENDING, CursorType


import test_constants


def get_oplog(uri, ts_start=None, ts_end=None, output_to_file=None):
    client = MongoClient(uri)
    local_db = client.get_database('local', read_preference=ReadPreference.PRIMARY)
    oplog = local_db.get_collection('oplog.rs')

    # Start/stop oplog at these timestamps.
    try:
        if ts_start:
            ts_start = Timestamp(datetime.fromisoformat(ts_start))
        else:
            first = oplog.find().sort('$natural', ASCENDING).limit(-1).next()
            ts_start = first['ts']
    except TypeError:
        print('Could not convert ts_start to Timestamp.')

    try:
        if ts_end:
            ts_end = Timestamp(datetime.fromisoformat(ts_end))
        else:
            t = time.time()
            ts_end = Timestamp(int(t), 0)
    except TypeError:
        print("Could not convert ts_end to Timestamp.")
        return

    print(f'Saving oplog from {ts_start} to {ts_end}')
    while True:
        # For a regular capped collection CursorType.TAILABLE_AWAIT is the
        # only option required to create a tailable cursor. When querying the
        # oplog, the oplog_replay option enables an optimization to quickly
        # find the 'ts' value we're looking for. The oplog_replay option
        # can only be used when querying the oplog. Starting in MongoDB 4.4
        # this option is ignored by the server as queries against the oplog
        # are optimized automatically by the MongoDB query engine.
        cursor = oplog.find({'ts': {'$gt': ts_start}},
                            cursor_type=CursorType.TAILABLE_AWAIT,
                            oplog_replay=True)
        while cursor.alive:
            for doc in cursor:
                ts = doc['ts']

                if not output_to_file:
                    print(doc)
                else:
                    output_to_file.write(str(doc))

                if ts_end and ts >= ts_end:
                    return

            # We end up here if the find() returned no documents or if the
            # tailable cursor timed out (no new documents were added to the
            # collection for more than 1 second).
            time.sleep(1)
            print('.')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='oplog_tail', description='Parses the oplog and outputs to stdout or file.')
    parser.add_argument('--uri', default=test_constants.test_uri)
    parser.add_argument('--out', type=argparse.FileType('w', encoding='UTF-8'))
    parser.add_argument('--start')
    parser.add_argument('--end')
    args = parser.parse_args()

    get_oplog(args.uri, ts_start=args.start, ts_end=args.end, output_to_file=args.out)
