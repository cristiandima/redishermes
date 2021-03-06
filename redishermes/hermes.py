import redis
import uuid
import time
import typing
from collections import namedtuple

Stats = namedtuple('Stats', ['pending', 'in_progress', 'expired'])


class RedisHermes:
    def __init__(self, r: redis.StrictRedis, q_name='hermes'):
        self.r = r
        self.q_name = 'hermes:{}_queue'.format(q_name)
        self.q_name_processing = 'hermes:{}_processing'.format(q_name)

    def _get_lock_name(self, msg_id: str) -> str:
        return 'hermes:lock:{}'.format(msg_id)

    def _lock_job(self, msg_id: str, revive_after):
        self.r.set(self._get_lock_name(msg_id), int(time.time() + revive_after))

    def _maybe_lock_job(self, msg_id: str, revive_after):
        self.r.setnx(self._get_lock_name(msg_id), int(time.time() + revive_after))

    def put(self, msg: str):
        """Put a new string message in queue"""
        msg_id = str(uuid.uuid4())
        pipe = self.r.pipeline()
        pipe.rpush(self.q_name, msg_id)
        pipe.set(msg_id, msg)
        pipe.execute()

    def get(self, revive_after=60) -> 'Message':
        """Blocking call to redis returning a new message whenever it becomes
        available"""
        msg_id = self.r.brpoplpush(self.q_name, self.q_name_processing)
        self._lock_job(msg_id, revive_after)
        data = self.r.get(msg_id)
        return Message(self, msg_id, data)

    def get_now(self, revive_after=60) -> typing.Union['Message', None]:
        """Non blocking call to redis returning a new message if any is available
        or None if none are available
        """
        msg_id = self.r.rpoplpush(self.q_name, self.q_name_processing)
        if msg_id is None:
            return None
        self._lock_job(msg_id, revive_after)
        data = self.r.get(msg_id)
        return Message(self, msg_id, data)

    def revive(self):
        """Call this periodically to revive any dead messages (messages received by
        workers which were not confirmed). In progress messages are stored in a
        different '_processing' list. If any message is left there after revive_after
        number of seconds have passed it gets put back in the queue"""
        in_progress = self.r.lrange(self.q_name_processing, 0, -1)
        for msg_id in in_progress:
            lock = self.r.get(self._get_lock_name(msg_id))
            if lock is not None and int(lock) < int(time.time()):
                # put the message back in q if lock expired
                pipe = self.r.pipeline()
                pipe.lrem(self.q_name_processing, 1, msg_id)
                pipe.rpush(self.q_name, msg_id)
                pipe.execute()
            # either in race condition with consumer or consumer died before
            # it could lock the message
            if lock is None:
                # give consumer 5 seconds to lock the job, but use the maybe
                # call to ensure we dont overwrite consumer lock
                self._maybe_lock_job(msg_id, revive_after=5)

    def stats(self) -> Stats:
        pending = self.r.llen(self.q_name)
        in_progress = self.r.llen(self.q_name_processing)

        in_progress_queue = self.r.lrange(self.q_name_processing, 0, -1)
        pipe = self.r.pipeline(transaction=False)
        for msg_id in in_progress_queue:
            pipe.get(self._get_lock_name(msg_id))

        locks = pipe.execute()
        now_time = time.time()
        expired = len([lock for lock in locks if lock is not None and int(lock) < int(now_time)])

        return Stats(pending=pending, in_progress=in_progress, expired=expired)


class Message:
    def __init__(self, courier: RedisHermes, msg_id: str, data: str):
        self.msg_id = msg_id
        self.data = data
        self.courier = courier

    def confirm(self):
        self.courier.r.lrem(self.courier.q_name_processing, 1, self.msg_id)
        self.courier.r.delete(self.msg_id)
