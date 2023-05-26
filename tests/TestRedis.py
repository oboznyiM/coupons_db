# Test redis
import unittest, random
import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from PostgreSQLProvider import PostgreSQLProvider
from PostgreSQLService import PostgreSQLService
from NotificationsService import NotificationService
from redisProvider import RedisProvider
from redisService import RedisService


class TestRedisFunctions(unittest.TestCase):
    def setUp(self):
        self.redisProvider = RedisProvider(True)
        self.notifications = NotificationService(self.redisProvider)
        self.dbProvider = PostgreSQLProvider()
        self.db = PostgreSQLService(self.dbProvider, self.notifications)
        self.redisService = RedisService(self.redisProvider, self.db)
        self.db.clear_schema()
        self.db.create_tables()
        self.db.fill_tables()

    def tearDown(self):
        pass

    def test_get_coupon(self):
        coupon_id = random.randint(1, 10)
        coupon = self.redisService.get_coupon(coupon_id)
        self.assertEqual(coupon, self.db.get_coupon_by_id(coupon_id))
        coupon = self.redisService.get_coupon(coupon_id)
        self.assertEqual(coupon, self.db.get_coupon_by_id(coupon_id))
        self.assertEqual(
            int(self.redisService.r.hget("maksym-coupon_access", coupon_id)), 2
        )


unittest.main()
