import uuid
import hashlib
from datetime import datetime
from lib.pg import PgConnect

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def _generate_hash(self, *fields) -> str:
        combined = "_".join(str(field) for field in fields)
        return hashlib.md5(combined.encode()).hexdigest()

    def hub_order_insert(self, order_id: str, order_dt: datetime) -> str:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT h_order_pk FROM dds.h_order WHERE order_id = %s", (order_id,))
                existing = cur.fetchone()
                if existing:
                    return existing[0]
                
                order_pk = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (order_pk, order_id, order_dt, datetime.utcnow(), 'kafka')
                )
                return order_pk

    def hub_user_insert(self, user_id: str) -> str:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT h_user_pk FROM dds.h_user WHERE user_id = %s", (user_id,))
                existing = cur.fetchone()
                if existing:
                    return existing[0]
                
                user_pk = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (user_pk, user_id, datetime.utcnow(), 'kafka')
                )
                return user_pk

    def hub_restaurant_insert(self, restaurant_id: str) -> str:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT h_restaurant_pk FROM dds.h_restaurant WHERE restaurant_id = %s", (restaurant_id,))
                existing = cur.fetchone()
                if existing:
                    return existing[0]
                
                restaurant_pk = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (restaurant_pk, restaurant_id, datetime.utcnow(), 'kafka')
                )
                return restaurant_pk

    def hub_product_insert(self, product_id: str) -> str:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT h_product_pk FROM dds.h_product WHERE product_id = %s", (product_id,))
                existing = cur.fetchone()
                if existing:
                    return existing[0]
                
                product_pk = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (product_pk, product_id, datetime.utcnow(), 'kafka')
                )
                return product_pk

    def hub_category_insert(self, category_name: str) -> str:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT h_category_pk FROM dds.h_category WHERE category_name = %s", (category_name,))
                existing = cur.fetchone()
                if existing:
                    return existing[0]
                
                category_pk = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (category_pk, category_name, datetime.utcnow(), 'kafka')
                )
                return category_pk

    def sat_order_cost_insert(self, order_pk: str, cost: float, payment: float) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                current_hash = self._generate_hash(cost, payment)
                
        
                cur.execute("""
                    SELECT hk_order_cost_hashdiff 
                    FROM dds.s_order_cost 
                    WHERE h_order_pk = %s 
                    ORDER BY load_dt DESC 
                    LIMIT 1
                """, (order_pk,))
                
                last_hash = cur.fetchone()
                
                if not last_hash or last_hash[0] != current_hash:
                    cur.execute(
                        """
                        INSERT INTO dds.s_order_cost 
                        (h_order_pk, cost, payment, load_dt, load_src, hk_order_cost_hashdiff)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (order_pk, cost, payment, datetime.utcnow(), 'kafka', current_hash)
                    )

    def sat_order_status_insert(self, order_pk: str, status: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                current_hash = self._generate_hash(status)
                
                cur.execute("""
                    SELECT hk_order_status_hashdiff 
                    FROM dds.s_order_status 
                    WHERE h_order_pk = %s 
                    ORDER BY load_dt DESC 
                    LIMIT 1
                """, (order_pk,))
                
                last_hash = cur.fetchone()
                
             
                if not last_hash or last_hash[0] != current_hash:
                    cur.execute(
                        """
                        INSERT INTO dds.s_order_status 
                        (h_order_pk, status, load_dt, load_src, hk_order_status_hashdiff)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (order_pk, status, datetime.utcnow(), 'kafka', current_hash)
                    )

    def sat_user_names_insert(self, user_pk: str, username: str, userlogin: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                current_hash = self._generate_hash(username, userlogin)
              
                cur.execute("""
                    SELECT hk_user_names_hashdiff 
                    FROM dds.s_user_names 
                    WHERE h_user_pk = %s 
                    ORDER BY load_dt DESC 
                    LIMIT 1
                """, (user_pk,))
                
                last_hash = cur.fetchone()
                
                if not last_hash or last_hash[0] != current_hash:
                    cur.execute(
                        """
                        INSERT INTO dds.s_user_names 
                        (h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (user_pk, username, userlogin, datetime.utcnow(), 'kafka', current_hash)
                    )

    def sat_restaurant_names_insert(self, restaurant_pk: str, name: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                current_hash = self._generate_hash(name)

                cur.execute("""
                    SELECT hk_restaurant_names_hashdiff 
                    FROM dds.s_restaurant_names 
                    WHERE h_restaurant_pk = %s 
                    ORDER BY load_dt DESC 
                    LIMIT 1
                """, (restaurant_pk,))
                
                last_hash = cur.fetchone()
 
                if not last_hash or last_hash[0] != current_hash:
                    cur.execute(
                        """
                        INSERT INTO dds.s_restaurant_names 
                        (h_restaurant_pk, name, load_dt, load_src, hk_restaurant_names_hashdiff)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (restaurant_pk, name, datetime.utcnow(), 'kafka', current_hash)
                    )

    def sat_product_names_insert(self, product_pk: str, name: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                current_hash = self._generate_hash(name)

                cur.execute("""
                    SELECT hk_product_names_hashdiff 
                    FROM dds.s_product_names 
                    WHERE h_product_pk = %s 
                    ORDER BY load_dt DESC 
                    LIMIT 1
                """, (product_pk,))
                
                last_hash = cur.fetchone()
                
                # Вставляем только если данные изменились
                if not last_hash or last_hash[0] != current_hash:
                    cur.execute(
                        """
                        INSERT INTO dds.s_product_names 
                        (h_product_pk, name, load_dt, load_src, hk_product_names_hashdiff)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (product_pk, name, datetime.utcnow(), 'kafka', current_hash)
                    )

    def link_order_product_insert(self, order_pk: str, product_pk: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM dds.l_order_product WHERE h_order_pk = %s AND h_product_pk = %s",
                    (order_pk, product_pk)
                )
                if not cur.fetchone():
                    cur.execute(
                        """
                        INSERT INTO dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (str(uuid.uuid4()), order_pk, product_pk, datetime.utcnow(), 'kafka')
                    )

    def link_order_user_insert(self, order_pk: str, user_pk: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM dds.l_order_user WHERE h_order_pk = %s AND h_user_pk = %s",
                    (order_pk, user_pk)
                )
                if not cur.fetchone():
                    cur.execute(
                        """
                        INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (str(uuid.uuid4()), order_pk, user_pk, datetime.utcnow(), 'kafka')
                    )

    def link_product_category_insert(self, product_pk: str, category_pk: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM dds.l_product_category WHERE h_product_pk = %s AND h_category_pk = %s",
                    (product_pk, category_pk)
                )
                if not cur.fetchone():
                    cur.execute(
                        """
                        INSERT INTO dds.l_product_category (hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (str(uuid.uuid4()), product_pk, category_pk, datetime.utcnow(), 'kafka')
                    )

    def link_product_restaurant_insert(self, product_pk: str, restaurant_pk: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM dds.l_product_restaurant WHERE h_product_pk = %s AND h_restaurant_pk = %s",
                    (product_pk, restaurant_pk)
                )
                if not cur.fetchone():
                    cur.execute(
                        """
                        INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (str(uuid.uuid4()), product_pk, restaurant_pk, datetime.utcnow(), 'kafka')
                    )
