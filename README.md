## Testing performance of change streams

Consists on four scripts:

populate.py
- Drop old database
- Create a baseline of documents

watch.py
- start changestream watch threads

operate.py
- run operations.

tail.py
- reads oplog