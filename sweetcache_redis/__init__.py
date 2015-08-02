import pickle

from sweetcache import NotFoundError
from redis import Redis


class RedisBackend(object):

    def __init__(self, **kwargs):
        self.redis = Redis(**kwargs)

    @staticmethod
    def _make_key(key):
        return ".".join(key)

    def set(self, key, value, expires):
        name = self._make_key(key)

        value = pickle.dumps(value)

        if expires is None:
            assert self.redis.set(
                name,
                value,
            )
        else:
            time = int(expires.total_seconds())

            assert self.redis.setex(
                name,
                value,
                time,
            )

    def get(self, key):
        value = self.redis.get(
            self._make_key(key),
        )

        if value is None:
            raise NotFoundError()

        value = pickle.loads(value)

        return value
