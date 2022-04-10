import abc
import json
import logging
from datetime import datetime
from json.decoder import JSONDecodeError
from typing import Any

from redis import Redis


def error_invalid_json(err: Exception):
    """Завершение программы, если JSON невалидный."""
    logging.error("Невалидный json:\n {e}".format(e=err))
    raise SystemExit(1)


class BaseStorage:
    """Абстрактный класс для описания интерфеса хранилища."""

    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище."""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища."""
        pass


class JsonFileStorage(BaseStorage):
    """Класс для работы c json-файлом в виде хранилища для работы с состоянием."""

    def __init__(self, file_path: str = "state.json"):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        try:
            with open(self.file_path, "w") as jsonfile:
                json.dump(state, jsonfile, indent=4, ensure_ascii=False)
        except JSONDecodeError as err:
            error_invalid_json(err)

    def retrieve_state(self) -> dict:
        try:
            with open(self.file_path, "r") as jsonfile:
                state = json.load(jsonfile)
        except (PermissionError) as err:
            logging.error(
                "Доступ к файлу отсутсвует: {p}\n {e}".format(
                    p=self.file_path,
                    e=err,
                ),
            )
            raise SystemExit(1)
        except JSONDecodeError as err:
            error_invalid_json(err)
        except FileNotFoundError:
            logging.warning("Файл не найден: {p}".format(p=self.file_path))
            state = {}
        return state


class RedisStorage(BaseStorage):
    """Класс для работы c Redis в виде хранилища для хранения текущего состояния."""

    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter
        self.state = redis_adapter.data.get("data") or {}

        if self.state:
            self.state = json.loads(self.state)
        self.save_state(self.state)

    def save_state(self, state: dict) -> None:
        self.redis_adapter.data["data"] = json.dumps(state)
        self.redis_adapter.set("data", json.dumps(state))
        self.state = state

    def retrieve_state(self) -> dict:
        return self.state


class State:
    """
    Класс для хранения текущего состояния.
    """

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage
        self.data = self.storage.retrieve_state()
        self.start_sync: str

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.data[key] = value
        self.storage.save_state(self.data)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        self.start_sync = str(datetime.now())
        return self.data.get(key)
