import requests
import json
import os
import logging
from lxml import html

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_article_info(title):
    print('info')
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

def get_articles_from_category(category):
    print('cat')
    try:
        params = {
            "action": "query",
            "format": "json",
            "list": "categorymembers",
            "cmtitle": category,
            "cmlimit": "max",
        }

        articles = []
        while True:
            response = requests.get("https://simple.wikipedia.org/w/api.php", params=params)
            data = response.json()
            articles.extend([page["title"] for page in data.get("query", {}).get("categorymembers", [])])
            if "continue" in data:
                params["cmcontinue"] = data["continue"]["cmcontinue"]
            else:
                break

        return articles

    except Exception as e:
        logger.error(f"Ошибка при получении статей из категории '{category}': {e}")
        return []

def save_article_info_to_json(article_info, title, folder):
    print('art')
    filename = f"{title.replace('/', '_')}.json"  # Заменяем слеши в названии, чтобы избежать проблем с файлами
    filepath = os.path.join(folder, filename)
    os.makedirs(folder, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as file:
        json.dump(article_info, file, ensure_ascii=False, indent=4)
    logger.info(f"Информация о статье '{title}' сохранена в файл '{filename}'")

def get_and_save_all_it_articles(folder):
    it_categories = [
        # "Category:Information_technology",
        # "Category:Computing",
        # "Category:Software",
        "Category:American_search_engines"
    ]

    for category in it_categories:
        articles = get_articles_from_category(category)
        for title in articles:
            article_info = get_article_info(title)
            if article_info:
                save_article_info_to_json(article_info, title, folder)

output_folder = "IT_Articles"
get_and_save_all_it_articles(output_folder)
