import logging
import uuid
from typing import Dict, Iterator

from elasticsearch import Elasticsearch
from elasticsearch.helpers import BulkIndexError, bulk

from etltoes.structs import Movie


class Saver(object):
    """Класс для сохранения данных по кинопроизведениям в ES"""

    def __init__(self, es_client: Elasticsearch) -> None:
        self.es_client = es_client
        self.movies_id: list[uuid.UUID] = []
        self.actions: list[dict] = []

    def _prepare_actions(self, movies: Iterator[Movie]) -> Iterator[Dict]:
        """Подготавливает документы для сохранения в ES.

        Arguments:
            movies --

        Yields:
            генератор actions для функции elasticsearch.helpers.bulk
        """
        for movie in movies:
            if movie.id not in self.movies_id:
                self.movies_id.append(movie.id)
                action = {
                    "_index": "movies",
                    "_id": str(movie.id),
                    "_source": {
                        "id": str(movie.id),
                        "imdb_rating": movie.rating,
                        "genre": movie.genres,
                        "title": movie.title,
                        "description": movie.description,
                        "director": movie.director,
                        "actors_names": movie.actors_names,
                        "writers_names": movie.writers_names,
                        "actors": movie.actors_id,
                        "writers": movie.writers_id,
                    },
                }
                self.actions.append(action)

        yield from self.actions

    def save_to_es(self, movies: Iterator[Movie]) -> None:
        try:
            stat = bulk(self.es_client, self._prepare_actions(movies=movies), stats_only=True)
            logging.info("Статус загрузки в ES: {a}".format(a=stat))
        except BulkIndexError as err:
            logging.error(err.errors[0])
