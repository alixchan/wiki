import unittest
from unittest.mock import patch, MagicMock
import requests_mock
import json
from kafka import get_article_info, save_article_info_to_mongodb, save_article_info_to_elk, get_and_save_all_articles

class TestWikipediaScraper(unittest.TestCase):
    
    @requests_mock.Mocker()
    def test_get_article_info(self, m):
        title = "Python_(programming_language)"
        # Mock the API responses
        m.get('https://simple.wikipedia.org/w/api.php', json={
            "parse": {
                "title": title,
                "pageid": 12345,
                "revid": 54321,
                "text": {"*": "<p>Python is a programming language</p>"}
            }
        })
        m.get('https://simple.wikipedia.org/w/api.php', json={
            "query": {
                "pages": {
                    "12345": {
                        "revisions": [],
                        "pageprops": {},
                        "pageviews": {}
                    }
                }
            }
        })

        article_info = get_article_info(title)
        self.assertIsNotNone(article_info)
        self.assertEqual(article_info["title"], title)
        self.assertEqual(article_info["pageid"], 12345)
        self.assertIn("Python is a programming language", article_info["text"])

    @patch('kafka.collection.insert_one')
    def test_save_article_info_to_mongodb(self, mock_insert_one):
        article_info = {"title": "Test Article", "pageid": 12345}
        save_article_info_to_mongodb(article_info)
        mock_insert_one.assert_called_once_with(article_info)

    @patch('kafka.es.index')
    def test_save_article_info_to_elk(self, mock_index):
        article_info = {"title": "Test Article", "pageid": 12345}
        save_article_info_to_elk(article_info)
        mock_index.assert_called_once_with(index="wikipedia", id=12345, document=article_info)

    @requests_mock.Mocker()
    @patch('kafka.get_article_info')
    @patch('kafka.save_article_info_to_mongodb')
    @patch('kafka.save_article_info_to_elk')
    def test_get_and_save_all_articles(self, m, mock_get_article_info, mock_save_article_info_to_mongodb, mock_save_article_info_to_elk):
        # Mock the API response for the list of articles
        m.get('https://simple.wikipedia.org/w/api.php', json={
            "query": {
                "allpages": [
                    {"title": "Article_1"},
                    {"title": "Article_2"}
                ]
            },
            "continue": {
                "apcontinue": "Article_3"
            }
        })

        mock_get_article_info.return_value = {"title": "Test Article", "pageid": 12345}
        
        get_and_save_all_articles()
        
        self.assertEqual(mock_get_article_info.call_count, 2)
        self.assertEqual(mock_save_article_info_to_mongodb.call_count, 2)
        self.assertEqual(mock_save_article_info_to_elk.call_count, 2)

if __name__ == '__main__':
    unittest.main()
