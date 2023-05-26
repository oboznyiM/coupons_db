import threading
import time
import redis


class DataMartUpdater:
    def __init__(self, cur, conn):
        self.cur = cur
        self.conn = conn
        self.r = redis.Redis(host="localhost", port=6379, db=0)
        self.update = False

    def update_data_mart(self):
        while self.update:
            # Get all keys that match the pattern
            keys = self.r.keys("coupon:*:sold_quantity")
            for key in keys:
                # Extract the coupon_id from the key
                coupon_id = key.decode("utf-8").split(":")[1]
                # Get the sold_quantity from Redis
                sold_quantity = int(self.r.get(key))
                # Update the Coupon_Usage table
                self.cur.execute(
                    """
                    UPDATE Coupon_Usage
                    SET sold_quantity = %s
                    WHERE coupon_id = %s
                    """,
                    (sold_quantity, coupon_id),
                )
                # Remove the key from Redis
                self.r.delete(key)

            self.conn.commit()
            # Wait for 10 seconds
            time.sleep(10)

    def start_updating(self):
        self.update = True
        self.worker_thread = threading.Thread(target=self.update_data_mart, daemon=True)
        self.worker_thread.start()

    def stop_updating(self):
        self.update = False
        if self.worker_thread.is_alive():
            self.worker_thread.join()
