import redis


class RedisProvider:
    def __init__(self, local):
        if not local:
            self.r = redis.Redis(
                host="redis-17072.c285.us-west-2-2.ec2.cloud.redislabs.com",
                port=17072,
                password="4PLTzbkfHThICHNiBqoL2PGnljfBK8bh",
            )
        else:
            self.r = redis.Redis(host="127.0.0.1", port=6379)
