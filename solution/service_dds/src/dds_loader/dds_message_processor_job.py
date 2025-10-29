from datetime import datetime
from logging import Logger
from dds_loader.repository.dds_repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self, consumer, producer, dds_repository: DdsRepository, logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
            try:
                self._process_single_message(msg)
            except Exception as e:
                self._logger.error(f"Error processing message: {e}")
                continue

    def _process_single_message(self, msg: dict) -> None:
        payload = msg["payload"]
        
        order_pk = self._dds_repository.hub_order_insert(
            str(msg["object_id"]),
            datetime.strptime(payload["date"], '%Y-%m-%d %H:%M:%S')
        )
        user_pk = self._dds_repository.hub_user_insert(payload["user"]["id"])
        restaurant_pk = self._dds_repository.hub_restaurant_insert(payload["restaurant"]["id"])
        
        self._dds_repository.sat_order_cost_insert(order_pk, payload["cost"], payload["payment"])
        self._dds_repository.sat_order_status_insert(order_pk, payload["status"])
        self._dds_repository.sat_user_names_insert(user_pk, payload["user"]["name"], payload["user"]["login"])
        self._dds_repository.sat_restaurant_names_insert(restaurant_pk, payload["restaurant"]["name"])
        self._dds_repository.link_order_user_insert(order_pk, user_pk)
        
        products_data = self._process_products(msg, order_pk, restaurant_pk)
        self._send_to_output_topic(msg, products_data, user_pk, restaurant_pk)

    def _process_products(self, msg: dict, order_pk: str, restaurant_pk: str) -> list:
        products_data = []
        for product in msg["payload"]["products"]:
            product_pk = self._dds_repository.hub_product_insert(product["id"])
            category_pk = self._dds_repository.hub_category_insert(product["category"])
            
            self._dds_repository.sat_product_names_insert(product_pk, product["name"])
            self._dds_repository.link_order_product_insert(order_pk, product_pk)
            self._dds_repository.link_product_category_insert(product_pk, category_pk)
            self._dds_repository.link_product_restaurant_insert(product_pk, restaurant_pk)
            
            products_data.append({
                "product_id": product_pk,
                "product_name": product["name"],
                "category_name": product["category"],
                "price": product["price"],
                "quantity": product["quantity"]
            })
        return products_data

    def _send_to_output_topic(self, msg: dict, products_data: list, user_pk: str, restaurant_pk: str) -> None:
        output_message = {
            "object_id": msg["object_id"],
            "object_type": "order",
            "payload": {
                "id": msg["object_id"],
                "date": msg["payload"]["date"],
                "cost": msg["payload"]["cost"],
                "payment": msg["payload"]["payment"],
                "status": msg["payload"]["status"],
                "user": {
                    "id": str(user_pk),
                    "name": msg["payload"]["user"]["name"]
                },
                "restaurant": {
                    "id": str(restaurant_pk),
                    "name": msg["payload"]["restaurant"]["name"]
                },
                "products": [{
                    "product_id": str(product["product_id"]),
                    "product_name": product["product_name"],
                    "category_name": product["category_name"],
                    "price": product["price"],
                    "quantity": product["quantity"]
                } for product in products_data],
                "processed_ts": datetime.utcnow().isoformat()
            }
        }
        self._producer.produce(output_message)
