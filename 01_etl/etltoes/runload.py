import logging
import os
import time
from contextlib import contextmanager
from itertools import chain
from typing import Iterator

import psycopg2
from dotenv import load_dotenv
from elasticsearch import ConnectionError, Elasticsearch
from psycopg2 import OperationalError
from psycopg2.extensions import connection as _conn
from psycopg2.extras import DictCursor

from etltoes.loader import FilmworkLoader, GenreLoader, PersonLoader, Tables
from etltoes.saver import Saver
from etltoes.state import JsonFileStorage, State


def backoff(
    exception,
    retry: int = 3,
    start_sleep_time: float = 0.1,
    factor: int = 2,
    border_sleep_time: int = 10,
    message: dict = {"error": "Ошибка."},
):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """
    count = 0

    def exp() -> Iterator[float]:
        nonlocal count
        while True:
            t = start_sleep_time * factor**count
            if t < border_sleep_time:
                count += 1
                yield t
            else:
                yield border_sleep_time

    def wrapper(func, *args, **kwargs):
        def inner(*args, **kwargs):
            count = 0
            errt: str = ""
            while True:
                if count >= retry:
                    raise exception(errt)
                try:
                    return func(*args, **kwargs)
                except exception as err:
                    errt = err
                    logging.error(message["error"])
                count += 1
                time.sleep(next(exp()))

        return inner

    return wrapper


@contextmanager
def conn_postgres(dsl: dict) -> Iterator[_conn]:
    """Менеджер для подключения к Postgres"""

    conn = None
    message = {"error": "Произошла ошибка подключения к БД. Пробуем подключиться еще раз."}

    @backoff(OperationalError, retry=200, message=message)
    def connect(dsl) -> _conn:
        conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        conn.cursor()
        return conn

    conn = connect(dsl)
    yield conn


@contextmanager
def conn_es(url: str) -> Iterator[Elasticsearch]:
    """Менеджер для подключения к ES"""

    conn = None
    message = {"error": "Произошла ошибка подключения к ES. Пробуем подключиться еще раз."}

    @backoff(ConnectionError, retry=200, message=message)
    def connect() -> Elasticsearch:
        conn = Elasticsearch(url)
        if not conn.ping(error_trace=False):
            raise ConnectionError("Не удается подключиться к ES")
        return conn

    conn = connect()
    yield conn


if __name__ == "__main__":
    log_format = "%(asctime)s %(levelname)s %(message)s"

    es_logger = logging.getLogger("elasticsearch")
    es_logger.setLevel(level=logging.WARNING)

    logging.basicConfig(format=log_format, level=logging.INFO, filename="etltoes.log", filemode="w")

    load_dotenv()

    dsl = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST", "127.0.0.1"),
        "port": os.environ.get("DB_PORT", 5432),
    }
    es_url = os.environ.get("ES_URL", "http://127.0.0.1:9200")

    try:
        with conn_postgres(dsl) as pg_conn, conn_es(es_url) as es_client:
            state_storage = JsonFileStorage()
            state = State(state_storage)
            saver = Saver(es_client=es_client)

            genre_loader = GenreLoader(pg_conn, state)
            filmwork_loader = FilmworkLoader(pg_conn, state)
            person_loader = PersonLoader(pg_conn, state)

            movies = chain(
                filmwork_loader.updated_movies(),
                genre_loader.updated_movies(),
                person_loader.updated_movies(),
            )
            saver.save_to_es(movies=movies)
            state.set_state(Tables.person.value, state.start_sync)
            state.set_state(Tables.genre.value, state.start_sync)
            state.set_state(Tables.filmwork.value, state.start_sync)

    except OperationalError as err:
        logging.error("Ошибка работы с БД\n {e}".format(e=err))
    except ConnectionError as err:
        logging.error("Ошибка работы с ES\n")
