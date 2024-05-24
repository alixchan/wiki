from pymongo import MongoClient
import nltk
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
import matplotlib.pyplot as plt
import time

nltk.download('punkt')
nltk.download('stopwords')

client = MongoClient('mongodb://192.168.0.95:27017', serverSelectionTimeoutMS=60000)
db = client.simple_wiki
collection = db.articles

start = time.time()
stop_words = set(stopwords.words('english'))

def extract_title(article): # для извлечения текста из статьи
    return article.get('title', '')


def get_titles(category):
    titles = []
    for article in collection.find({"categories": {"$elemMatch": {"*": category}}}, {"title": 1}):
        title = extract_title(article) # для извлечения заголовков статей из категории "Stars"
        if title:
            titles.append(title)
    return titles

category = "Software"
titles = get_titles(category)

vectorizer = TfidfVectorizer(stop_words=list(stop_words)) 
X = vectorizer.fit_transform(titles)

k = 2
knn = NearestNeighbors(n_neighbors=k, metric='cosine')
knn.fit(X)

distances, indices = knn.kneighbors(X)

plt.figure(figsize=(10, 6))
plt.title("KNN Clustering of 'Software' Article Titles")
for i in range(len(titles)):
    plt.scatter(indices[i], [i]*k, alpha=0.6)
plt.xlabel("Neighbor Index")
plt.ylabel("Article Index")
plt.show()
plt.savefig('knn_title.png')


print(time.time() - start)

# Этот код:

# Извлекает признаки (длину статьи и количество внешних ссылок) и целевую переменную (количество редакций) из статей категории "Stars".
# Преобразует данные в формат DataFrame.
# Удаляет строки с отсутствующими значениями.
# Разделяет данные на обучающий и тестовый наборы.
# Обучает модель линейной регрессии на обучающем наборе данных.
# Предсказывает значения целевой переменной на тестовом наборе данных.
# Оценивает качество модели с помощью среднеквадратичной ошибки и коэффициента детерминации.