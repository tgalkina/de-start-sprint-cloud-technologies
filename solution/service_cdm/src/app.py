import logging
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from cdm_loader.cdm_message_processor_job import CdmMessageProcessor
from cdm_loader.repository.cdm_repository import CdmRepository

app = Flask(__name__)

config = AppConfig()

@app.get('/health')
def hello_world():
    return 'healthy'

if __name__ == '__main__':
    app.logger.setLevel(logging.INFO)
    
    # Инициализация компонентов
    consumer = config.kafka_consumer()
    pg_connect = config.pg_warehouse_db()
    cdm_repository = CdmRepository(pg_connect)

    # Инициализация процессора
    processor = CdmMessageProcessor(
        consumer=consumer,
        cdm_repository=cdm_repository,
        db=pg_connect,
        logger=app.logger
    )

    # Запуск планировщика
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=processor.run, trigger="interval", seconds=25)
    scheduler.start()

    app.logger.info("CDM Service started successfully")
    app.run(debug=False, host='0.0.0.0', port=5000, use_reloader=False)
