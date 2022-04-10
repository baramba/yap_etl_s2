"""Описание основных структур для перноса данных из SQLite в Postgres."""

import uuid
from typing import Optional

from pydantic import BaseModel, Extra, validator


class Movie(BaseModel):
    """Класс для хранения информации о кинопроизведениях"""

    id: uuid.UUID
    rating: Optional[float] = 0.0
    title: str
    description: str = ""
    genres: Optional[tuple] = tuple()
    director: list = list()
    actors: Optional[tuple] = tuple()
    writers: Optional[tuple] = tuple()
    actors_names: list = list()
    writers_names: list = list()
    actors_id: list = []
    writers_id: list = []

    class Config:
        validate_assignment = True
        extra = Extra.forbid

    @validator("actors", pre=True)
    def set_actors(cls, actors):
        return actors or tuple()

    @validator("writers", pre=True)
    def set_writers(cls, writers):
        return writers or tuple()

    @validator("director", pre=True)
    def set_director(cls, director):
        return director or list()

    def __init__(self, **data):
        super().__init__(**data)

        # актеры и писатели приходят из базы в формате - name^id
        # тут мы их делим и перекладываем в соответсвующие поля класса

        for actorid in self.actors:
            name, id = str(actorid).split("^", 1)
            self.actors_id.append({"id": id, "name": name})
            self.actors_names.append(name)

        for writerid in self.writers:
            name, id = str(writerid).split("^", 1)
            self.writers_id.append({"id": id, "name": name})
            self.writers_names.append(name)
