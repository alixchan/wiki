from kafka import KafkaProducer, KafkaConsumer
import json

# Создание продюсера Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Определение тем Kafka
mongo_topic = 'mongo_topic'
elasticsearch_topic = 'elasticsearch_topic'

# Отправка сообщений в Kafka
def send_to_kafka(data, topic):
    producer.send(topic, value=data)

# Подключение к теме Kafka для MongoDB
mongo_consumer = KafkaConsumer(mongo_topic,
                               bootstrap_servers='localhost:9092',
                               auto_offset_reset='earliest',
                               enable_auto_commit=True,
                               group_id='mongo_group',
                               value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Подключение к теме Kafka для ElasticSearch
elasticsearch_consumer = KafkaConsumer(elasticsearch_topic,
                                       bootstrap_servers='localhost:9092',
                                       auto_offset_reset='earliest',
                                       enable_auto_commit=True,
                                       group_id='elasticsearch_group',
                                       value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Сохранение в MongoDB
def save_to_mongodb(data):
    try:
        collection.insert_one(data)
        logger.info(f"Информация о статье '{data['title']}' сохранена в MongoDB")
    except Exception as e:
        logger.error(f"Ошибка при сохранении информации о статье '{data['title']}' в MongoDB: {e}")

# Сохранение в ElasticSearch
def save_to_elasticsearch(data):
    try: 
        es.index(index="wikipedia", id=data["pageid"], document=data) 
        logger.info(f"Информация о статье '{data['title']}' сохранена в Elasticsearch")
    except Exception as e:
        logger.error(f"Ошибка при сохранении информации о статье '{data['title']}' в Elasticsearch: {e}")

# Получение и обработка данных из Kafka
def process_data_from_kafka(consumer, save_func):
    for message in consumer:
        data = message.value
        save_func(data)

# Функция для отправки данных в Kafka и сохранения их в базах данных
def process_and_send(article_info):
    send_to_kafka(article_info, mongo_topic)
    send_to_kafka(article_info, elasticsearch_topic)
    logger.info(f"Информация о статье '{article_info['title']}' отправлена в Kafka")

# Обработка всех статей
def get_and_save_all_articles():
    try:
        params = {
            "action": "query",
            "format": "json",
            "list": "allpages",
            "aplimit": "max",
        }

        while True:
            response = requests.get("https://simple.wikipedia.org/w/api.php", params=params)
            data = response.json()           

            articles_info = [page["title"] for page in data.get("query", {}).get("allpages", [])]
            
            for title in articles_info:
                article_info = get_article_info(title)
                if article_info:
                    process_and_send(article_info)

            if "continue" in data:
                params["apcontinue"] = data["continue"]["apcontinue"]
            else:
                break

    except Exception as e:
        logger.error(f"Ошибка при получении списка статей: {e}")

# Запуск обработки статей и сохранение в Kafka
get_and_save_all_articles()
process_data_from_kafka(mongo_consumer, save_to_mongodb)
process_data_from_kafka(elasticsearch_consumer, save_to_elasticsearch)
