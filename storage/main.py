from pymongo import MongoClient
from collections import Counter
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt


# Подключение к MongoDB
client = MongoClient('mongodb://192.168.0.95:27017', serverSelectionTimeoutMS=220000)
db = client.simple_wiki
collection = db.articles


def articles_distribution_by_letter():
    pipeline = [
        {"$project": {"first_letter": {"$substrCP": ["$title", 0, 1]}}},
        {"$group": {"_id": "$first_letter", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    return list(collection.aggregate(pipeline))

def top_categories():
    pipeline = [
        {"$unwind": "$categories"},
        {"$group": {"_id": "$categories", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    return list(collection.aggregate(pipeline))

def categories_distribution():
    pipeline = [
        {"$unwind": "$categories"},
        {"$group": {"_id": "$categories", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    return list(collection.aggregate(pipeline))

def top_templates():
    pipeline = [
        {"$unwind": "$templates"},
        {"$group": {"_id": "$templates", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    return list(collection.aggregate(pipeline))

def templates_distribution():
    pipeline = [
        {"$unwind": "$templates"},
        {"$group": {"_id": "$templates", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    return list(collection.aggregate(pipeline))

def top_articles_by_length():
    pipeline = [
        {"$sort": {"length": -1}},
        {"$limit": 10},
        {"$project": {"title": 1, "length": 1, "_id": 0}}
    ]
    return list(collection.aggregate(pipeline))

def length_distribution():
    pipeline = [
        {"$bucketAuto": {"groupBy": "$length", "buckets": 10}},
        {"$project": {"_id": 0, "min": "$_id.min", "max": "$_id.max", "count": "$count"}}
    ]
    return list(collection.aggregate(pipeline))

def top_articles_by_langlinks():
    pipeline = [
        {"$project": {"title": 1, "langlinks_count": {"$size": "$langlinks"}}},
        {"$sort": {"langlinks_count": -1}},
        {"$limit": 10}
    ]
    return list(collection.aggregate(pipeline))

def langlinks_distribution():
    pipeline = [
        {"$unwind": "$langlinks"},
        {"$group": {"_id": "$langlinks.lang", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    return list(collection.aggregate(pipeline))

def top_articles_by_links():
    pipeline = [
        {"$project": {"title": 1, "links_count": {"$size": "$links"}}},
        {"$sort": {"links_count": -1}},
        {"$limit": 10}
    ]
    return list(collection.aggregate(pipeline))

def top_articles_by_categories():
    pipeline = [
        {"$project": {"title": 1, "categories_count": {"$size": "$categories"}}},
        {"$sort": {"categories_count": -1}},
        {"$limit": 10}
    ]
    return list(collection.aggregate(pipeline))

def top_articles_by_externallinks():
    pipeline = [
        {"$project": {"title": 1, "externallinks_count": {"$size": "$externallinks"}}},
        {"$sort": {"externallinks_count": -1}},
        {"$limit": 10}
    ]
    return list(collection.aggregate(pipeline))

def top_articles_by_images():
    pipeline = [
        {"$project": {"title": 1, "images_count": {"$size": "$images"}}},
        {"$sort": {"images_count": -1}},
        {"$limit": 10}
    ]
    return list(collection.aggregate(pipeline))

def top_articles_by_sections():
    pipeline = [
        {"$project": {"title": 1, "sections_count": {"$size": "$sections"}}},
        {"$sort": {"sections_count": -1}},
        {"$limit": 10}
    ]
    return list(collection.aggregate(pipeline))

def top_articles_by_pageviews():
    date_30_days_ago = (datetime.utcnow() - timedelta(days=30)).strftime('%Y%m%d00')
    pipeline = [
        {"$project": {"title": 1, "pageviews": {"$objectToArray": "$pageviews"}}},
        {"$unwind": "$pageviews"},
        {"$match": {"pageviews.k": {"$gte": date_30_days_ago}}},
        {"$group": {"_id": "$title", "total_views": {"$sum": "$pageviews.v"}}},
        {"$sort": {"total_views": -1}},
        {"$limit": 10}
    ]
    return list(collection.aggregate(pipeline))

def top_articles_by_revisions():
    pipeline = [
        {"$project": {"title": 1, "revisions_count": {"$size": "$revisions"}}},
        {"$sort": {"revisions_count": -1}},
        {"$limit": 10}
    ]
    return list(collection.aggregate(pipeline))

def top_editors():
    pipeline = [
        {"$unwind": "$revisions"},
        {"$group": {"_id": "$revisions.user", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    return list(collection.aggregate(pipeline))

def editors_distribution():
    pipeline = [
        {"$unwind": "$revisions"},
        {"$group": {"_id": "$revisions.user", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    return list(collection.aggregate(pipeline))

def articles_growth_over_time():
    pipeline = [
        {"$unwind": "$revisions"},
        {"$group": {"_id": {"$substr": ["$revisions.timestamp", 0, 10]}, "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    df = pd.DataFrame(result)
    df['_id'] = pd.to_datetime(df['_id'])
    df = df.set_index('_id').resample('M').sum()
    return df

def articles_growth_over_time():
    pipeline = [
        {"$unwind": "$revisions"},
        {"$group": {"_id": {"$substr": ["$revisions.timestamp", 0, 10]}, "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    df = pd.DataFrame(result)
    df['_id'] = pd.to_datetime(df['_id'])
    df = df.set_index('_id').resample('D').sum().cumsum()  # Ежедневное накопление статей
    return df.reset_index()

def top_articles_by_pageviews():
    date_30_days_ago = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    pipeline = [
        {"$project": {
            "title": 1,
            "pageviews": {
                "$objectToArray": "$pageviews"
            }
        }},
        {"$unwind": "$pageviews"},
        {"$match": {
            "pageviews.k": {"$gte": date_30_days_ago}
        }},
        {"$group": {
            "_id": "$title",
            "total_views": {"$sum": "$pageviews.v"}
        }},
        {"$sort": {"total_views": -1}},
        {"$limit": 10}
    ]
    return list(collection.aggregate(pipeline))



# Вызов функций в параллельных потоках и запись результатов в Excel
if __name__ == "__main__":
    functions = {
        "1": articles_distribution_by_letter, #Распределение статей по буквам
        "2": top_categories, #Самые часто используемые категории (топ 10)
        "3": categories_distribution, #Распределение категорий по частоте использования
        "4": top_templates, #Самые популярные шаблоны (топ 10)
        "5": templates_distribution, #Распределение шаблонов по частоте использования
        "6": top_articles_by_length, #Самые большие по размеру статьи (топ 10)
        "7": length_distribution, #Распределение статей по размеру
        "8": top_articles_by_langlinks, #Статьи, где больше всего языков (топ 10)
        "9": langlinks_distribution, #Распределение языков статей
        "10": top_articles_by_links, #Статьи, где больше всего ссылок (топ 10)
        "11": top_articles_by_categories, #Статьи, где больше всего категорий (топ 10)
        "12": top_articles_by_externallinks, #Статьи, где больше всего внешних ссылок (топ 10)
        "13": top_articles_by_images, #Статьи, где больше всего изображений (топ 10)
        "14": top_articles_by_sections, #Статьи с самой подробной sections/оглавлением (топ 10)
        "15": articles_growth_over_time, 
        "16": top_articles_by_revisions, #Статьи с наибольшим количеством ревизий (топ 10)
        "17": top_editors, #Самые активные редакторы (топ 10)
        "18": editors_distribution, #Распределение редакторов по количеству правок
        "19": articles_growth_over_time, #Рост количества статей со временем
        "20": top_articles_by_pageviews
    }


    results = {}
    execution_times = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(func): name for name, func in functions.items()}
        for future in as_completed(futures):
            name = futures[future]
            start_time = time.time()
            try:
                result = future.result()
                if isinstance(result, pd.DataFrame):
                    results[name] = result
                else:
                    results[name] = pd.DataFrame(result)
            except Exception as e:
                results[name] = pd.DataFrame({'Error': [str(e)]})
            execution_time = (time.time() - start_time)
            execution_times.append({"Function": name, "Execution Time (s)": execution_time})
            print(f"{name} выполнена за {execution_time:.3f} секунд(ы)")

    # Создание Excel-файла
    with pd.ExcelWriter('simple_wiki.xlsx') as writer:
        for sheet_name, df in results.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
