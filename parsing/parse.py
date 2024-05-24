import requests
import json
import os
import logging
from bs4 import BeautifulSoup
from lxml import html
from pymongo import MongoClient
from elasticsearch import Elasticsearch

# Конфигурирование логгера
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Подключение к MongoDB
client = MongoClient('mongodb://192.168.0.95:27017', serverSelectionTimeoutMS=60000)
db = client.simple_wiki
collection = db.articles

# Elastic
es = Elasticsearch('http://localhost:9200', timeout=5)

def get_article_info(title):
    try:
        params = {
            "action": "parse",
            "page": title,
            "format": "json",     
        }
        response = requests.get("https://simple.wikipedia.org/w/api.php", params=params)
        data = response.json()
        text_html = data["parse"]["text"]["*"]

        tree = html.fromstring(text_html)
        text = tree.text_content()
        
        revisions = []
        params_revisions = {
            "action": "query",
            "format": "json",
            "titles": title,
            "prop": "revisions",
            "rvprop": "ids|timestamp|size|flags|comment|user",
            "rvdir": "newer",
            "rvlimit": 500  # Устанавливаем начальный лимит на 500 ревизий
        }

        while True:
            response_revisions = requests.get("https://simple.wikipedia.org/w/api.php", params=params_revisions)
            data_revisions = response_revisions.json()
            page_id = next(iter(data_revisions["query"]["pages"]))
            if "revisions" in data_revisions["query"]["pages"][page_id]:
                revisions.extend(data_revisions["query"]["pages"][page_id]["revisions"])
            if "continue" in data_revisions:
                params_revisions["rvcontinue"] = data_revisions["continue"]["rvcontinue"]
            else:
                break

        params_pageprops = {
            "action": "query",
            "format": "json",
            "titles": title,
            "prop": "info|pageprops|pageviews"
        }

        response_pageprops = requests.get("https://simple.wikipedia.org/w/api.php", params=params_pageprops)
        data_pageprops = response_pageprops.json() 
        pageprops = data_pageprops["query"]["pages"][page_id] 

        article_info = {
            "title": title,
            "pageid": data["parse"].get("pageid"),
            "revid": data["parse"].get("revid"),
            "text": text,
            "langlinks": data["parse"].get("langlinks", []),
            "categories": data["parse"].get("categories", []),
            "links": data["parse"].get("links", []),
            "templates": data["parse"].get("templates", []),
            "images": data["parse"].get("images", []),
            "externallinks": data["parse"].get("externallinks", []),
            "sections": data["parse"].get("sections", []),
            "showtoc": data["parse"].get("showtoc"),
            "displaytitle": data["parse"].get("displaytitle"),
            "iwlinks": data["parse"].get("iwlinks", []),
            "properties": data["parse"].get("properties", {}),
            "length": data_pageprops["query"]["pages"][page_id].get("length"),
            "pageprops": pageprops.get("pageprops", {}),
            "pageviews": pageprops.get("pageviews", {}),
            "revisions": revisions
        }
        return article_info

    except Exception as e:
        logger.error(f"Ошибка при получении информации о статье '{title}': {e}")
        return None

def get_and_save_all_articles():
    try:
        params = {
            "action": "query",
            "format": "json",
            "list": "allpages",
            "aplimit": "max",
            # "apcontinue": "Hillsborough"
        }

        while True:
            response = requests.get("https://simple.wikipedia.org/w/api.php", params=params)
            data = response.json()           

            articles_info = [page["title"] for page in data.get("query", {}).get("allpages", [])]
            
            for title in articles_info:
                article_info = get_article_info(title)
                if article_info:
                    save_article_info_to_mongodb(article_info)
                    save_article_info_to_elk(article_info)

            if "continue" in data:
                params["apcontinue"] = data["continue"]["apcontinue"]
            else:
                break

    except Exception as e:
        logger.error(f"Ошибка при получении списка статей: {e}")


def save_article_info_to_elk(article_info):
    try: 
        es.index(index="wikipedia", id=article_info["pageid"], document=article_info) 
        logger.info(f"Информация о статье '{article_info['title']}' сохранена в Elasticsearch")
    except Exception as e:
        logger.error(f"Ошибка при сохранении информации о статье '{article_info['title']}' в Elasticsearch: {e}")


def save_article_info_to_mongodb(article_info):
    try:
        collection.insert_one(article_info)
        logger.info(f"Информация о статье '{article_info['title']}' сохранена в MongoDB")
    except Exception as e:
        logger.error(f"Ошибка при сохранении информации о статье '{article_info['title']}' в MongoDB: {e}")

get_and_save_all_articles()
