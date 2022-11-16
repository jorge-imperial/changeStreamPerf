import datetime

from time import sleep
from multiprocessing import Process

import populate_db
from operate import run_all_operations
from tail import get_oplog
from watch import watch

if __name__ == "__main__":
    #
    test_uri = 'mongodb+srv://root:P4ssw0rd@realmcluster.hqulf.mongodb.net/?retryWrites=true&w=majority'
    test_db = 'watched'
    test_files = [('four', 'four.json')]

    start = datetime.datetime.now().isoformat()

    print('Populating database ')
    populate_db.populate(test_uri, test_db, test_files)

    # sleep 5 minutes
    print('Wait a few minutes for replication...')
    sleep(1*60)

    # Start watching database
    watch_proc = Process(target=watch, args=(test_uri, test_db, 'watch.log'))

    # run operations
    operations_proc = Process(target=run_all_operations, args=(test_uri, "four.json", 'four',
                                                               400000, 4000,
                                                               'updates.json', 'inserts.json', 'deletes.json'))
    print('Start watch() on database')
    watch_proc.start()
    sleep(30)
    print('Start operations')
    operations_proc.start()

    # Wait until operations finish and capture the oplog.
    print('Waiting for operations to finish...')
    operations_proc.join()
    print('Operations finished!')

    end = datetime.datetime.now().isoformat()

    print('Get oplog')
    get_oplog(test_uri, ts_start=start, ts_end=end, oplog_file_name='oplog.json')

    print('Done')
