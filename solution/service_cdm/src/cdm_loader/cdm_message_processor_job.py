from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository
from lib.pg import PgConnect
import logging

class CdmMessageProcessor:
    def __init__(self, consumer: KafkaConsumer, cdm_repository: CdmRepository, db: PgConnect, logger) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._db = db
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        processed_count = 0
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                self._logger.info("No more messages to process")
                break
            
            self._process_message(msg)
            processed_count += 1
        
        if processed_count > 0:
            self._logger.info(f"Processed {processed_count} messages")

    def _process_message(self, msg: dict) -> None:
        try:
            self._logger.debug(f"Received message: {msg}")
            payload = msg.get('payload', {})
            
            user_info = payload.get('user', {})
            products = payload.get('products', [])
            
            user_id = user_info.get('id')
            if not user_id:
                self._logger.warning("No user_id in message payload")
                return

            if not products:
                self._logger.warning(f"No products found for user: {user_id}")
                return

            self._logger.info(f"Processing user: {user_id} with {len(products)} products")
            
            self._cdm_repository.insert_user_product_counters(user_id, products)
            self._cdm_repository.insert_user_category_counters(user_id, products)
            
            self._logger.info(f"Successfully processed user: {user_id}")

        except Exception as e:
            self._logger.error(f"Error processing message: {e}")
            import traceback
            self._logger.error(traceback.format_exc())
