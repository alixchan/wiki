from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
import nltk
from nltk.corpus import stopwords
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import time

# Загрузка необходимых ресурсов NLTK
nltk.download('punkt')
nltk.download('stopwords')

# Подключение к MongoDB
client = MongoClient('mongodb://192.168.0.95:27017', serverSelectionTimeoutMS=60000)
db = client.simple_wiki
collection = db.articles

start = time.time()

stop_words = set(stopwords.words('english'))

def process_text(text):
    words = nltk.word_tokenize(text)
    words = [word.lower() for word in words if word.isalnum()]
    words = [word for word in words if word not in stop_words]
    return words

def extract_text(article):
    return article.get('text', '')

# для параллельного извлечения и обработки текста
def get_word_counts(category):
    word_counts = Counter()
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(extract_text, article) for article in collection.find({"categories": {"$elemMatch": {"*": category}}}, {"text": 1})]
        for future in as_completed(futures):
            text = future.result()
            if text:  # Проверка на наличие текста
                words = process_text(text)
                word_counts.update(words)
    return word_counts

category = "Computer"
word_counts = get_word_counts(category)

if word_counts:  # Проверка на наличие слов
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(word_counts)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.show()
    plt.savefig('cloud_Software.png')
else:
    print(f"No words found for the category '{category}'")

print(time.time() - start)

# О коде:

# Подключаемся к базе данных и коллекции articles.
# Настраиваем список стоп-слов для удаления из текста.
# Функция process_text выполняет токенизацию текста, преобразует слова в нижний регистр, 
# удаляет небуквенно-цифровые символы и стоп-слова.
# Функция extract_text извлекает текст из статьи.
# Функция get_word_counts выполняет параллельное извлечение текста из статей 
# определенной категории с использованием ThreadPoolExecutor и обновляет счетчик слов Counter.
# В основной функции задается категория, вызывается функция для получения счетчика слов,
# создается карта слов и отображается с помощью matplotlib.

