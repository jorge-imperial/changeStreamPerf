## Testing performance of change streams

Consists on four scripts:

populate.py
- Drop old database
- Create collections and insert documents.

watch.py
- start changestream watch threads.

operate.py
- run operations.

tail.py
- reads oplog

Using the logs created from the last three steps, gather time at which operations were executed, time at which they were inserted in the oplog and time where the watch operation in the changestream detected them.

-----------------
Test clusters 
Cluster0 (5.0.13)
Cluster1 (4.4.17)
Cluster2 (6.0.)


```
def run_test():

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


```


