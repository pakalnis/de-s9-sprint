import os

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.pg import PgConnect
from lib.redis import RedisClient


class AppConfig:
    _CERTIFICATE_PATH = '/crt/YandexInternalRootCA.crt'
    DEFAULT_JOB_INTERVAL = 25

    def __init__(self) -> None:

        self._kafka_host = str(os.getenv('KAFKA_HOST') or "")
        self._kafka_port = int(str(os.getenv('KAFKA_PORT')) or 0)
        self._kafka_consumer_username = str(os.getenv('KAFKA_USER') or "")
        self._kafka_consumer_password = str(os.getenv('KAFKA_WORD') or "")
        self._kafka_consumer_group = str(os.getenv('KAFKA_GROUP') or "")
        self._kafka_consumer_topic = str(os.getenv('KAFKA_SRC_TOPIC') or "")
        self._kafka_producer_username = str(os.getenv('KAFKA_USER') or "")
        self._kafka_producer_password = str(os.getenv('KAFKA_WORD') or "")
        self._kafka_producer_topic = str(os.getenv('KAFKA_DST_TOPIC') or "")

        self._redis_host = str(os.getenv('REDIS_HOST') or "")
        self._redis_port = int(str(os.getenv('REDIS_PORT')) or 0)
        self._redis_password = str(os.getenv('REDIS_WORD') or "")

        self._pg_warehouse_host = str(os.getenv('BASE_HOST') or "")
        self._pg_warehouse_port = int(str(os.getenv('BASE_PORT') or 0))
        self._pg_warehouse_dbname = str(os.getenv('BASE_NAME') or "")
        self._pg_warehouse_user = str(os.getenv('BASE_USER') or "")
        self._pg_warehouse_password = str(os.getenv('BASE_WORD') or "")

    def kafka_producer(self):
        return KafkaProducer(
            self._kafka_host,
            self._kafka_port,
            self._kafka_producer_username,
            self._kafka_producer_password,
            self._kafka_producer_topic,
            self._CERTIFICATE_PATH
        )

    def kafka_consumer(self):
        return KafkaConsumer(
            self._kafka_host,
            self._kafka_port,
            self._kafka_consumer_username,
            self._kafka_consumer_password,
            self._kafka_consumer_topic,
            self._kafka_consumer_group,
            self._CERTIFICATE_PATH
        )

    def redis_client(self) -> RedisClient:
        return RedisClient(
            self._redis_host,
            self._redis_port,
            self._redis_password,
            self._CERTIFICATE_PATH
        )

    def pg_warehouse_db(self):
        return PgConnect(
            self._pg_warehouse_host,
            self._pg_warehouse_port,
            self._pg_warehouse_dbname,
            self._pg_warehouse_user,
            self._pg_warehouse_password
        )
