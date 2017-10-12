# redishermes

Redishermes is a simple redis backed message queue written in python3.6.

```Python
import redis
from redishermes import RedisHermes

# in clients
r = redis.StrictRedis() # your redis instance
messager = RedisHermes(r) # client object
messager.put('Hello World !!!') # add a message to the queue

# in background workers
# blocking variant
while True:
  msg = messager.get() # blocking call to redis waiting for new messages
  print(msg.data) # returned msg value is a Message object exposing data via property
  msg.confirm() # message is deleted from redis, worked finished its work

# non-blocking variant
msg = messager.get_now() # will return a Message or None
if msg is not None:
  print(msg.data)
  msg.confirm()

# optional revive_after param (default 60 seconds)
# if a message is not confirmed after revive_after seconds the miracle
# worker below will be able to revive it (aka put it back in queue)
msg = messager.get_now(revive_after=10)

# miracle worker
while True:
  messager.revive() # will put dead messages back in queue
  sleep(x) # call revive periodically to revive dead jobs
```
