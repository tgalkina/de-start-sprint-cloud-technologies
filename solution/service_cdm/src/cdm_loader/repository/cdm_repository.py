from typing import List
from lib.pg import PgConnect
import uuid

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert_user_product_counters(self, user_id: str, products: List[dict]) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for product in products:
                    product_id = product.get('product_id')
                    product_name = product.get('product_name')
                    quantity = product.get('quantity', 1)
                    
                    if not all([user_id, product_id, product_name]):
                        continue
                    cur.execute(
                        """
                        INSERT INTO cdm.user_product_counters 
                            (user_id, product_id, product_name, order_cnt)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (user_id, product_id) 
                        DO UPDATE SET 
                            order_cnt = cdm.user_product_counters.order_cnt + EXCLUDED.order_cnt,
                            product_name = EXCLUDED.product_name
                        """,
                        (user_id, product_id, product_name, quantity)
                    )

    def insert_user_category_counters(self, user_id: str, products: List[dict]) -> None:
        category_data = {}
        
        for product in products:
            category_name = product.get('category_name')
            quantity = product.get('quantity', 1)
            
            if not category_name:
                continue
                
            category_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"category_{category_name}"))
            
            if category_id not in category_data:
                category_data[category_id] = {
                    'category_id': category_id,
                    'category_name': category_name,
                    'order_cnt': 0
                }
            category_data[category_id]['order_cnt'] += quantity

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for category in category_data.values():
                    cur.execute(
                        """
                        INSERT INTO cdm.user_category_counters 
                            (user_id, category_id, category_name, order_cnt)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (user_id, category_id) 
                        DO UPDATE SET 
                            order_cnt = cdm.user_category_counters.order_cnt + EXCLUDED.order_cnt,
                            category_name = EXCLUDED.category_name
                        """,
                        (user_id, category['category_id'], category['category_name'], category['order_cnt'])
                    )
