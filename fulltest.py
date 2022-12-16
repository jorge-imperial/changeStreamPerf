import datetime

from time import sleep
from multiprocessing import Process

import populate_db
from operate import run_all_operations
from tail import get_oplog
from watch import watch

if __name__ == "__main__":
    # 4.4.17
    #test_uri = 'mongodb+srv://root:P4ssw0rd@cluster1.hqulf.mongodb.net/?retryWrites=true&w=majority'
    # 5.0.13
    #test_uri = 'mongodb+srv://root:P4ssw0rd@cluster0.hqulf.mongodb.net/?retryWrites=true&w=majority'
    # 6.0.2
    test_uri = 'mongodb+srv://root:P4ssw0rd@cluster2.hqulf.mongodb.net/?retryWrites=true&w=majority'

    test_db = 'watched'
    test_files = [('four', 'four.json')]

    start = datetime.datetime.now().isoformat()

    print('Populating database ')
    populate_db.populate(test_uri, test_db, test_files)

    # sleep 5 minutes
    print('Wait a few minutes for replication...')
    sleep(2*60)

    # Start watching database
    watch_proc = Process(target=watch, args=(test_uri, test_db, "watch_60.json",))
    print('Start watch() on database')
    watch_proc.start()

    sleep(30)

    # run operations
    operations_proc = Process(target=run_all_operations, args=(test_uri, "four.json", 'four',
                                                               400000, 40000,
                                                               'updates60.json', 'inserts60.json', 'deletes60.json',))

    print('Start operations')
    operations_proc.start()

    # Wait until operations finish and capture the oplog.
    print('Waiting for operations to finish...')
    operations_proc.join()
    print('Operations finished!')
    operations_proc.close()

    # End watch process
    watch_proc.kill()
    watch_proc.join()
    watch_proc.close()

    end = datetime.datetime.now().isoformat()
    print('Get oplog')
    get_oplog(test_uri, ts_start=start, ts_end=end, oplog_file_name='oplog60.json',)

    print('Done')
