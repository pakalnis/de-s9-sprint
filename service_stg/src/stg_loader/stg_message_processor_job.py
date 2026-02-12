import time
from datetime import datetime
from json import dumps
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size if batch_size else 100
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        # Имитация работы. Здесь будет реализована обработка сообщений.
        time.sleep(2)
        counter = 0
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if msg:
                object_id = msg.get('object_id')
                object_type = msg.get('object_type')
                sent_dttm = msg.get('sent_dttm')
                payload = msg.get('payload')
                if object_type == 'order' and payload and payload.get('final_status') == 'CLOSED':
                    # 2. Сохранение сообщения в БД postgresql (sprint9dwh.stg.order_events)
                    self._stg_repository.order_events_insert(
                        object_id=object_id,
                        object_type=object_type,
                        sent_dttm=sent_dttm,
                        payload=dumps(payload),
                    )
                    # 3. Достаньте id пользователя из сообщения и получите полную информацию о пользователе из Redis.
                    user_id = payload['user']['id']
                    user_data = self._redis.get(user_id)
                    # 4. Достаньте id ресторана из сообщения и получите полную информацию о ресторане из Redis.
                    restaurant_id = payload['restaurant']['id']
                    restaurant_data = self._redis.get(restaurant_id)

                    # Добавляем категорию в продукты
                    order_items = payload.get('order_items')
                    # Создаем словарик меню ресторана
                    menu_data = {item['_id']: item for item in restaurant_data['menu']}
                    # Обновляем продукты в заказе (order_items), добавляем категорию
                    [item.update({'category': menu_data[item['id']]['category']}) for item in order_items]

                    # 5. Сформируйте выходное сообщение.
                    out_msg = {
                        "object_id": object_id,
                        "object_type": object_type,
                        "payload": {
                            "id": object_id,
                            "date": payload.get('date'),
                            "cost": payload.get('cost'),
                            "payment": payload.get('payment'),
                            "status": payload.get('final_status'),
                            "restaurant": {
                                "id": restaurant_id,
                                "name": restaurant_data.get('name')
                            },
                            "user": {
                                "id": user_id,
                                "name": user_data.get('name'),
                                "login": user_data.get('login')
                            },
                            "products": order_items
                        }
                    }
                    # 6. Отправьте выходное сообщение в producer.
                    self._producer.produce(out_msg)
                    counter += 1
                    self._logger.info(f'Processed order_id {object_id}')
            else:
                break
        self._logger.info(f'Messages in batch: {counter}')

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
