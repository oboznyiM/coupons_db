import random
from hashlib import sha256
from faker import Faker
from utils import haversine
import redis


class PostgreSQLService:
    def __init__(self, provider, notifications):
        self.notificationsService = notifications
        self.conn = provider.conn
        self.cur = self.conn.cursor()
        self.r = redis.Redis(host="localhost", port=6379, db=0)
        self.fake = Faker()
        self.cur.execute("SET search_path TO maksymschema")
        self.conn.commit()
        self.clear_schema()
        self.create_tables()

    def clear_schema(self):
        self.cur.execute(
            """
            SELECT 'DROP TABLE IF EXISTS "' || tablename || '" CASCADE;' 
            FROM pg_tables 
            WHERE schemaname = 'maksymschema'
            """
        )
        drop_tables_commands = self.cur.fetchall()
        for command in drop_tables_commands:
            self.cur.execute(command[0])
        self.conn.commit()

    def create_tables(self):
        self.cur.execute(
            """
        CREATE TABLE IF NOT EXISTS maksymschema.Client (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            balance REAL NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL
        )
        """
        )

        self.cur.execute(
            """
        CREATE TABLE IF NOT EXISTS Owner (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        )
        """
        )

        self.cur.execute(
            """
        CREATE TABLE IF NOT EXISTS Venue (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            owner_id INTEGER,
            FOREIGN KEY(owner_id) REFERENCES Owner(id)
        )
        """
        )

        self.cur.execute(
            """
        CREATE TABLE IF NOT EXISTS Coupon (
            id SERIAL PRIMARY KEY,
            description TEXT NOT NULL,
            conditions TEXT,
            original_price REAL NOT NULL,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            owner_id INTEGER,
            FOREIGN KEY(owner_id) REFERENCES Owner(id)
        )
        """
        )

        self.cur.execute(
            """
        CREATE TABLE IF NOT EXISTS Offer (
            id SERIAL PRIMARY KEY,
            description TEXT NOT NULL,
            author_id INTEGER,
            FOREIGN KEY(author_id) REFERENCES Client(id)
        )
        """
        )

        self.cur.execute(
            """
        CREATE TABLE IF NOT EXISTS Venue_Coupon (
            venue_id INTEGER,
            coupon_id INTEGER,
            FOREIGN KEY(venue_id) REFERENCES Venue(id),
            FOREIGN KEY(coupon_id) REFERENCES Coupon(id)
        )
        """
        )

        self.cur.execute(
            """
        CREATE TABLE IF NOT EXISTS Venue_Offer (
            venue_id INTEGER,
            offer_id INTEGER,
            FOREIGN KEY(venue_id) REFERENCES Venue(id),
            FOREIGN KEY(offer_id) REFERENCES Offer(id)
        )
        """
        )

        self.cur.execute(
            """
        CREATE TABLE IF NOT EXISTS Client_Coupon (
            client_id INTEGER,
            coupon_id INTEGER,
            FOREIGN KEY(client_id) REFERENCES Client(id),
            FOREIGN KEY(coupon_id) REFERENCES Coupon(id)
        )
        """
        )
        self.conn.commit()

    def fill_tables(self):
        for _ in range(10):
            hashed_password = sha256(self.fake.password().encode()).hexdigest()
            self.cur.execute(
                "INSERT INTO Client (name, email, password, balance, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    self.fake.name(),
                    self.fake.email(),
                    hashed_password,
                    random.uniform(0, 1000),
                    random.uniform(-90, 90),
                    random.uniform(-180, 180),
                ),
            )
        for _ in range(10):
            hashed_password = sha256(self.fake.password().encode()).hexdigest()
            self.cur.execute(
                "INSERT INTO Owner (name, email, password) VALUES (%s, %s, %s)",
                (self.fake.name(), self.fake.email(), hashed_password),
            )
        for _ in range(10):
            self.cur.execute(
                "INSERT INTO Venue (name, latitude, longitude, owner_id) VALUES (%s, %s, %s, %s)",
                (
                    self.fake.company(),
                    random.uniform(-90, 90),
                    random.uniform(-180, 180),
                    random.randint(1, 10),
                ),
            )
        for _ in range(10):
            self.cur.execute(
                "INSERT INTO Coupon (description, original_price, quantity, price, owner_id) VALUES (%s, %s, %s, %s, %s)",
                (
                    self.fake.text(),
                    random.uniform(5, 100),
                    random.randint(1, 100),
                    random.uniform(1, 50),
                    random.randint(1, 10),
                ),
            )
        for _ in range(10):
            self.cur.execute(
                "INSERT INTO Offer (description, author_id) VALUES (%s, %s)",
                (self.fake.text(), random.randint(1, 10)),
            )
        for _ in range(10):
            self.cur.execute(
                "INSERT INTO Venue_Coupon (venue_id, coupon_id) VALUES (%s, %s)",
                (random.randint(1, 10), random.randint(1, 10)),
            )
        for _ in range(10):
            self.cur.execute(
                "INSERT INTO Venue_Offer (venue_id, offer_id) VALUES (%s, %s)",
                (random.randint(1, 10), random.randint(1, 10)),
            )
        for _ in range(10):
            self.cur.execute(
                "INSERT INTO Client_Coupon (client_id, coupon_id) VALUES (%s, %s)",
                (random.randint(1, 10), random.randint(1, 10)),
            )
        self.conn.commit()

    def create_and_populate_data_mart_table(self):
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS Coupon_Usage (
                coupon_id INTEGER PRIMARY KEY,
                original_price REAL NOT NULL,
                discount_price REAL NOT NULL,
                total_quantity INTEGER NOT NULL,
                sold_quantity INTEGER,
                owner_id INTEGER,
                FOREIGN KEY(owner_id) REFERENCES Owner(id)
            )
            """
        )
        self.cur.execute(
            """
            INSERT INTO Coupon_Usage (coupon_id, original_price, discount_price, total_quantity, sold_quantity, owner_id)
            SELECT 
                Coupon.id,
                Coupon.original_price,
                Coupon.price AS discount_price,
                Coupon.quantity AS total_quantity,
                COUNT(Client_Coupon.coupon_id) AS sold_quantity,
                Coupon.owner_id
            FROM 
                Coupon
            LEFT JOIN 
                Client_Coupon ON Coupon.id = Client_Coupon.coupon_id
            GROUP BY 
                Coupon.id
            """
        )
        self.conn.commit()

    def print_data_mart(self):
        self.cur.execute("SELECT * FROM Coupon_Usage")
        rows = self.cur.fetchall()
        print(
            "coupon_id, original_price, discount_price, total_quantity, sold_quantity, owner_id"
        )
        for row in rows:
            print(row)

        self.conn.commit()

    def purchase_coupon(self, client_id, coupon_id):
        self.cur.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        try:
            self.cur.execute(
                "SELECT quantity, price FROM Coupon WHERE id = %s", (coupon_id,)
            )
            quantity, price = self.cur.fetchone()
            if quantity > 0:
                self.cur.execute(
                    "SELECT balance FROM Client WHERE id = %s", (client_id,)
                )
                balance = self.cur.fetchone()[0]
                if balance >= price:
                    new_balance = balance - price
                    new_quantity = quantity - 1
                    self.cur.execute(
                        "UPDATE Client SET balance = %s WHERE id = %s",
                        (new_balance, client_id),
                    )
                    self.cur.execute(
                        "UPDATE Coupon SET quantity = %s WHERE id = %s",
                        (new_quantity, coupon_id),
                    )
                    self.cur.execute(
                        "INSERT INTO Client_Coupon (client_id, coupon_id) VALUES (%s, %s)",
                        (client_id, coupon_id),
                    )
                    self.r.incr(f"coupon:{coupon_id}:sold_quantity")
                else:
                    print("Insufficient balance!")
            else:
                print("Coupon not available!")
            self.conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
            self.conn.rollback()

    # This one is slow but should be fine for demonstration purposes
    def view_nearby_venues(self, client_id, max_distance_km):
        self.cur.execute("BEGIN")
        self.cur.execute(
            "SELECT latitude, longitude FROM Client WHERE id = %s", (client_id,)
        )
        client_lat, client_lon = self.cur.fetchone()

        self.cur.execute("SELECT id, name, latitude, longitude FROM Venue")
        venues = self.cur.fetchall()
        self.conn.commit()

        nearby_venues = []
        for venue in venues:
            venue_id, venue_name, venue_lat, venue_lon = venue
            distance = haversine(client_lon, client_lat, venue_lon, venue_lat)
            if distance <= max_distance_km:
                nearby_venues.append((venue_id, venue_name, distance))

        return nearby_venues

    def use_coupon(self, client_id, coupon_id):
        self.cur.execute("BEGIN")
        self.cur.execute(
            "SELECT * FROM Client_Coupon WHERE client_id = %s AND coupon_id = %s",
            (client_id, coupon_id),
        )
        if self.cur.fetchone() is not None:
            self.cur.execute(
                "DELETE FROM Client_Coupon WHERE client_id = %s AND coupon_id = %s",
                (client_id, coupon_id),
            )
            print("Coupon used successfully!")
        else:
            print("The client does not own this coupon!")
        self.conn.commit()

    def set_offer_venue(self, venue_id, coupon_id, r):
        self.notificationsService.r.publish(f"maksym-venues:{venue_id}", coupon_id)
        self.cur.execute(
            "INSERT INTO Venue_Coupon (venue_id, coupon_id) VALUES (%s, %s)",
            (venue_id, coupon_id),
        )

    def publish_offer(self, client_id, description):
        self.cur.execute("BEGIN")
        self.cur.execute(
            "INSERT INTO Offer (description, author_id) VALUES (%s, %s)",
            (description, client_id),
        )
        self.conn.commit()

    def publish_coupon(self, owner_id, description, original_price, quantity, price):
        self.cur.execute("BEGIN")
        self.cur.execute(
            "INSERT INTO Coupon (description, original_price, quantity, price, owner_id) VALUES (%s, %s, %s, %s, %s)",
            (description, original_price, quantity, price, owner_id),
        )
        self.conn.commit()

    def view_coupons_for_venue(self, venue_id):
        self.cur.execute("BEGIN")
        self.cur.execute(
            "SELECT Coupon.id, Coupon.description, Coupon.original_price, Coupon.quantity, Coupon.price FROM Coupon INNER JOIN Venue_Coupon ON Coupon.id = Venue_Coupon.coupon_id WHERE Venue_Coupon.venue_id = %s",
            (venue_id,),
        )
        coupons = self.cur.fetchall()
        self.conn.commit()
        return coupons

    def get_coupon_by_id(self, coupon_id):
        try:
            self.cur.execute("BEGIN")
            self.cur.execute("SELECT * FROM Coupon WHERE id = %s", (coupon_id,))
            result = self.cur.fetchone()
            self.conn.commit()
            return result
        except Exception as e:
            print(f"An error occurred: {e}")
            self.conn.rollback()

    def stop(self):
        self.cur.close()
        self.conn.close()
