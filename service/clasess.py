import logging
import pandas as pd

logger = logging.getLogger('Classes')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class Recommendations:
    def __init__(self):
        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0,
        }

    def load(self, type_rec, path, **kwargs):
        """
        Загружает рекомендации из файла
        """
        logger.info(f"Loading recommendations, type: {type_rec}")
        self._recs[type_rec] = pd.read_parquet(path, **kwargs)
        if type_rec == "personal":
            self._recs[type_rec] = self._recs[type_rec].set_index("user_id")
        logger.info(f"Loaded {type_rec} recommendations")

    def get(self, user_id: int, k: int=100):
        """
        Возвращает список рекомендаций для пользователя
        """
        logger.debug(f'Запрос рекомендаций для user_id={user_id}, количество={k}')
        try:
            recs = self._recs["personal"].loc[user_id]
            recs = recs["track_id"].to_list()[:k]
            self._stats["request_personal_count"] += 1
        except KeyError:
            recs = self._recs["default"]
            recs = recs["track_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
        except Exception as e:
            logger.error(f"No recommendations found for user_id={user_id}. Error: {str(e)}")
            recs = []
        logger.info(f"Stats: {self._stats}")
        return recs

    def stats(self):
        logger.info("Stats for recommendations:")
        for name, value in self._stats.items():
            logger.info(f"{name} {value}")

class EventStore:
    def __init__(self, max_events_per_user=10):
        self.events = {}
        self.max_events_per_user = max_events_per_user

    def load(self, path, **kwargs):
        """
        Загружаем данные из файла
        """
        logger.info(f"Loading similar items from {path}")
        try:
            self.events = pd.read_parquet(path, **kwargs).set_index('user_id')
            logger.info("Loaded similar items")
        except Exception as e:
            logger.error(f"Error loading similar items from {path}. Error: {str(e)}")


    def get(self, user_id, k):
        """
        Возвращает события для пользователя
        """
        logger.debug(f'Запрос событий для user_id={user_id}, количество={k}')
        user_events = self.events.get(user_id, [])
        
        return user_events[:k]
    
class SimilarItems:
    def __init__(self):
        self._similar_items = None

    def load(self, path, **kwargs):
        """
        Загружаем данные из файла
        """
        logger.info(f"Loading similar items from {path}")
        try:
            self._similar_items = pd.read_parquet(path, **kwargs).set_index('track_id')
            logger.info("Loaded similar items")
        except Exception as e:
            logger.error(f"Error loading similar items from {path}. Error: {str(e)}")

    def get(self, item_id: int, k: int = 10):
        """
        Возвращает список похожих объектов
        """
        logger.debug(f'Запрос похожих объектов для item_id={item_id}, количество={k}')
        try:
            i2i = self._similar_items.loc[item_id].head(k)
            i2i = i2i[["track_id_recommended", "score"]].to_dict(orient="list")
        except KeyError:
            logger.error(f"No similar items found for item_id={item_id}")
            i2i = {"track_id_recommended": [], "score": []}
        except Exception as e:
            logger.error(f"Error retrieving similar items for item_id={item_id}. Error: {str(e)}")
            i2i = {"track_id_recommended": [], "score": []}

        return i2i
