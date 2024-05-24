from pymongo import MongoClient
import numpy as np
import pandas as pd
import time
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, r2_score

client = MongoClient('mongodb://192.168.0.95:27017', serverSelectionTimeoutMS=60000)
db = client.simple_wiki
collection = db.articles

start = time.time()

data = []
for article in collection.find({"categories": {"$elemMatch": {"*": "Software"}}}, {
    "length": 1, "externallinks": 1, "revisions": 1, "links": 1, "images": 1, "sections": 1, "pageviews": 1
}):
    length = article.get('length', 0)
    externallinks = len(article.get('externallinks', []))
    revisions = len(article.get('revisions', []))  # Считаем количество ревизий
    links = len(article.get('links', []))  # Считаем количество ссылок
    images = len(article.get('images', []))  # Считаем количество изображений
    sections = len(article.get('sections', []))  # Считаем количество секций
    pageviews = sum(article.get('pageviews', {}).values())  # Суммируем просмотры страницы
    data.append((length, externallinks, revisions, links, images, sections, pageviews))

# Создание DataFrame
df = pd.DataFrame(data, columns=['length', 'externallinks', 'revisions', 'links', 'images', 'sections', 'pageviews'])

# Удаление строк с отсутствующими значениями
df.dropna(inplace=True)

# Определение признаков и целевой переменной
X = df[['length', 'externallinks', 'links', 'images', 'sections', 'pageviews']]
y = df['revisions']

# Разделение данных на обучающий и тестовый наборы
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Настройка модели GradientBoostingRegressor с использованием GridSearchCV
param_grid = {
    'n_estimators': [100, 200],
    'learning_rate': [0.01, 0.1],
    'max_depth': [3, 5]
}

model = GradientBoostingRegressor(random_state=42)
grid_search = GridSearchCV(estimator=model, param_grid=param_grid, cv=3, scoring='r2', n_jobs=-1)
grid_search.fit(X_train, y_train)

best_model = grid_search.best_estimator_

# Предсказание на тестовом наборе данных
y_pred = best_model.predict(X_test)

# Оценка качества модели
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)
print(f'Best Parameters: {grid_search.best_params_}')
print(f'Mean Squared Error: {mse}')
print(f'R^2 Score: {r2}')

# Построение графика
plt.figure(figsize=(10, 6))
plt.scatter(y_test, y_pred, edgecolor='k', alpha=0.7)
plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2)
plt.xlabel('Actual Revisions')
plt.ylabel('Predicted Revisions')
plt.title('Actual vs Predicted Revisions')
plt.show()
