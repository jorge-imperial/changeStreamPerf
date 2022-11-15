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

------------------

Run populate.py
Wait 5 minutes.
Start watch.py 
Start operate.py
Wait for finish or operate.py and run tail.py

With logs do:
xxx



