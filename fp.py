import random
from PostgreSQLProvider import PostgreSQLProvider
from PostgreSQLService import PostgreSQLService
from NotificationsService import NotificationService
from redisProvider import RedisProvider
from redisService import RedisService
from SubscriberThread import Subscriber
from DataMartUpdater import DataMartUpdater

redisProvider = RedisProvider(True)
notifications = NotificationService(redisProvider)
dbProvider = PostgreSQLProvider()
db = PostgreSQLService(dbProvider, notifications)
redisService = RedisService(redisProvider, db)


# DEMO
db.clear_schema()

db.create_tables()
db.fill_tables()
db.create_and_populate_data_mart_table()

print("Data mart before:")
db.print_data_mart()

db.purchase_coupon(1, 2)
db.use_coupon(1, 2)
db.publish_offer(1, "This is a new offer!")
db.publish_coupon(1, "This is a new coupon!", 10.0, 5, 8.0)
coupons = db.view_coupons_for_venue(1)
print(coupons)


# Benchmarking redis
import time


def simulate_user():
    coupon_id = random.randint(1, 10)
    return redisService.get_coupon(coupon_id)


start = time.time()
for i in range(100):
    simulate_user()
end = time.time()
print("Time for 100 was: ", end - start)

start = time.time()
for i in range(10000):
    simulate_user()
end = time.time()
print("Time for 10000 was: ", end - start)


# reducing cash
print("Before reducing: ", redisService.r.keys("*"))
redisService.reduce_cash()
print("After reducing: ", redisService.r.keys("*"))


subscriber = Subscriber(1, redisService.r)
subscriber.start()


db.set_offer_venue(1, 5, redisService.r)
db.set_offer_venue(2, 6, redisService.r)
db.set_offer_venue(1, 7, redisService.r)

# Task 4
updater = DataMartUpdater(db.cur, db.conn)
db.r.set("coupon:1:sold_quantity", 10)
db.r.set("coupon:2:sold_quantity", 20)
updater.start_updating()
time.sleep(15)
updater.stop_updating()
print("Data mart after:")
db.print_data_mart()


db.stop()
subscriber.stop()
