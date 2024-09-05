import os
import logging
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI

from clasess import EventStore, SimilarItems, Recommendations

DATA_DIR = os.getenv('DATA_DIR', '../parquets')

logger = logging.getLogger("uvicorn.error")

events_store = EventStore()
sim_items_store = SimilarItems()
rec_store = Recommendations()

events_store.load(
    os.path.join(DATA_DIR,"events_train_sample.parquet"),
    columns=["user_id", "track_id", "track_seq"],
)

sim_items_store.load(
    os.path.join(DATA_DIR,"als_I2I_recommendations_sample.parquet"),
    columns=["track_id", "track_id_recommended", "score"],
)

rec_store.load(
    "personal",
    os.path.join(DATA_DIR,"als_recommendations_sample.parquet"),
    columns=["user_id", "track_id", "score"],
    )

rec_store.load(
    "default",
    os.path.join(DATA_DIR,"top_popular.parquet"),
    columns=["track_id", "score"],
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    logger.info("Starting")
    yield
    # этот код выполнится только один раз при остановке сервиса
    logger.info("Stopping")
    
# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)

async def recommendations_offline(user_id: int, k: int = 10):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """
    recs = rec_store.get(user_id, k)
    return {"recs": recs}

async def recommendations_online(user_id: int, k: int = 1):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
    """

    user_events = events_store.get(user_id, k)

    # получаем список похожих объектов
    if len(user_events) > 0:
        item_id = user_events[0]
        recs = sim_items_store.get(item_id)['track_id_recommended']
    else:
        recs = []

    return {"recs": recs}


@app.get("/")
async def test():
    return {'status': 'Hello world!'}

@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 10):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """
    recs_offline = await recommendations_offline(user_id, k)
    recs_online = await recommendations_online(user_id, k)

    recs_offline = recs_offline["recs"]
    recs_online = recs_online["recs"]

    recs_blended = []
    min_length = min(len(recs_offline), len(recs_online))
    # чередуем элементы из списков, пока позволяет минимальная длина
    for i in range(min_length):
        # ваш код здесь #
        if i % 2==0:
            recs_blended.append(recs_offline[i])
        else:
            recs_blended.append(recs_online[i])
    if len(recs_offline) < len(recs_online):
        recs_blended += recs_online[min_length:]
    else:
        recs_blended += recs_offline[min_length:]

    return {"recs": recs_blended}