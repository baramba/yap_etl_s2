""" Загрузка данных из Postgres пачками(размер = fetch_size)"""
import logging
from datetime import datetime
from enum import Enum
from typing import Iterator

from psycopg2.extensions import connection as _conn

from etltoes.state import State
from etltoes.structs import Movie


class Tables(Enum):
    filmwork = "film_work"
    genre = "genre"
    person = "person"


class Loader(object):
    """Абстрактный класс для получение списка обновленных фильмов."""

    def __init__(self, conn: _conn, state: State, fetch_size: int = 100) -> None:
        self.conn = conn
        self.state = state
        self.fetch_size = fetch_size
        self.movies_sql: str = ""
        self.table: str = ""
        movies: list[Movie] = []

    def updated_ids(self, table: str) -> "Iterator[tuple[str]]":
        """Генерирует последовательность списков идентификаторов, обновленных с последней синхронизации."""
        last_sync = self.state.get_state(table) or datetime(1900, 1, 1, 1, 1, 1, 0)
        sql = "select id from content.{t} where modified >= '{l}'".format(t=table, l=last_sync)
        cur = self.conn.cursor()
        cur.execute(sql)
        while True:
            ids = cur.fetchmany(size=self.fetch_size)
            if not ids:
                break
            values = tuple("".join(_id) for _id in ids)
            logging.info("Обновленных записей в таблице {t}: {c}".format(t=table, c=len(ids)))
            yield values

    def updated_movies(self) -> Iterator[Movie]:
        """Получить список обновленных фильмов."""

        cur = self.conn.cursor()

        for uuids in self.updated_ids(self.table):
            sql: bytes = cur.mogrify(self.movies_sql, (uuids,))
            cur.execute(sql)
            while True:
                movies: list[Movie] = []
                rows = cur.fetchmany(size=self.fetch_size)
                if not rows:
                    break
                for kwargs in rows:
                    movies.append(Movie(**kwargs))
                logging.info("Получено изменений по кинопроизведениям: {c}".format(c=len(movies)))
                yield from movies


class PersonLoader(Loader):
    """Получение списка обновленных фильмов на основании изменения в таблице person."""

    def __init__(self, conn: _conn, state: State, fetch_size: int = 1000) -> None:
        self.conn = conn
        self.state = state
        self.fetch_size = fetch_size
        self.table = Tables.person.value
        self.movies_sql = """
        select
            fw.id, 
            fw.title,
            fw.description,
            fw.rating,
            ARRAY_AGG(distinct g."name") as "genres",
            ARRAY_AGG(distinct p.full_name)  filter (where pfw."role" = 'director') as "director",
            ARRAY_AGG(distinct p.full_name ||'^'|| p.id) filter (
            where pfw."role" = 'actor') as "actors",
            ARRAY_AGG(distinct p.full_name ||'^'|| p.id) filter (
            where pfw."role" = 'writer') as "writers"
        from
            "content".person p
        left outer join "content".person_film_work pfw on pfw.person_id = p.id
        left outer join "content".film_work fw on fw.id = pfw.film_work_id
        left outer join "content".genre_film_work gfw on gfw.film_work_id = fw.id
        left outer join "content".genre g on g.id = gfw.genre_id
            where
                p.id in %s
            group by fw.id
        """


class GenreLoader(Loader):
    """Получение списка обновленных фильмов на основании изменения в таблице genre."""

    def __init__(self, conn: _conn, state: State, fetch_size: int = 1000) -> None:
        self.conn = conn
        self.state = state
        self.fetch_size = fetch_size
        self.table = Tables.genre.value
        self.movies_sql = """
        select
            fw.id, 
            fw.title,
            fw.description,
            fw.rating,
            ARRAY_AGG(distinct g."name") as "genres",
            ARRAY_AGG(distinct p.full_name)  filter (where pfw."role" = 'director') as "director",
            ARRAY_AGG(distinct p.full_name ||'^'|| p.id) filter (
            where pfw."role" = 'actor') as "actors",
            ARRAY_AGG(distinct p.full_name ||'^'|| p.id) filter (
            where pfw."role" = 'writer') as "writers"
        from
            "content".genre g 
        left join "content".genre_film_work gfw on gfw.genre_id  = g.id
        left join "content".film_work fw on fw.id = gfw.film_work_id 
        left join "content".person_film_work pfw on pfw.film_work_id = fw.id 
        left join "content".person p on p.id = pfw.person_id
        where
            g.id in %s
        group by fw.id
        """


class FilmworkLoader(Loader):
    """Получение списка обновленных фильмов на основании изменения в таблице film_work."""

    def __init__(self, conn: _conn, state: State, fetch_size: int = 1000) -> None:
        self.conn = conn
        self.state = state
        self.fetch_size = fetch_size
        self.table = Tables.filmwork.value
        self.movies_sql = """
        select
            fw.id, 
            fw.title,
            fw.description,
            fw.rating,
            ARRAY_AGG(distinct g."name") as "genres",
            ARRAY_AGG(distinct p.full_name)  filter (where pfw."role" = 'director') as "director",
            ARRAY_AGG(distinct p.full_name ||'^'|| p.id) filter (
            where pfw."role" = 'actor') as "actors",
            ARRAY_AGG(distinct p.full_name ||'^'|| p.id) filter (
            where pfw."role" = 'writer') as "writers"
        from
            "content".film_work fw 
        left join "content".person_film_work pfw on pfw.film_work_id = fw.id 
        left join "content".person p on p.id = pfw.person_id
        left join "content".genre_film_work gfw on gfw.film_work_id = fw.id
        left join "content".genre g on g.id = gfw.genre_id
        where
            fw.id  in %s
        group by fw.id
        """
