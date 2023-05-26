import json


class RedisService:
    def __init__(self, provider, dbService):
        self.db = dbService
        self.r = provider.r

    def clear(self):
        self.r.delete("maksym-coupon_access")
        for i in range(1, 11):
            self.r.delete(f"maksym-coupon:{i}")

    def get_coupon(self, coupon_id):
        coupon = self.r.get(f"maksym-coupon:{coupon_id}")
        if coupon is not None:
            self.r.hset(
                "maksym-coupon_access",
                coupon_id,
                int(self.r.hget("maksym-coupon_access", coupon_id)) + 1,
            )

            return tuple(json.loads(coupon))

        coupon = self.db.get_coupon_by_id(coupon_id)
        self.r.set(f"maksym-coupon:{coupon_id}", json.dumps(coupon))
        self.r.hset("maksym-coupon_access", coupon_id, 1)
        return coupon

    def reduce_cash(self):
        coupon_keys = self.r.keys("maksym-coupon:*")
        coupons = [
            (
                -int(
                    self.r.hget("maksym-coupon_access", json.loads(self.r.get(key))[0])
                ),
                self.r.get(key),
            )
            for key in coupon_keys
        ]

        while len(coupons) > 5:
            coupon = coupons[-1][1]
            coupon = json.loads(coupon)[0]
            self.r.hdel("maksym-coupon_access", coupon)
            self.r.delete(f"maksym-coupon:{coupon}")
            coupons.pop()
        for data in coupons:
            coupon = data[1]
            coupon = json.loads(coupon)[0]
            self.r.hset("maksym-coupon_access", coupon, 0)
