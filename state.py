import abc
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from config import app


json_file_name: str = app.state_file_name
file_path: str = str(Path(__file__).resolve().parent)


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        # Сохранить состояние
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        # Загрузить состояние
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict = {}) -> None:
        # Сохранить состояние
        file_state = self.retrieve_state()
        with open(f"{self.file_path}{json_file_name}", "w") as storage:
            save_state = {**file_state, **state}
            json.dump(save_state, storage, ensure_ascii=False, indent=4)

    def retrieve_state(self) -> dict:
        # Загрузить состояние
        with open(f"{self.file_path}{json_file_name}", "r") as storage:
            file = storage.read()
            if file == '':
                return {}
            else:
                state: dict = json.loads(file)
                return state


class State:
    """Класс для реализации состояния, при котором перенос
    данных из PG в ES был отказоустойчивым"""

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        # Установить состояние для определённого ключа
        self.storage.save_state(state={key: value})

    def get_state(self, key: str) -> Any:
        # Получить состояние по определённому ключу
        modified = self.storage.retrieve_state().get(key, None)
        if modified:
            return modified
        else:
            return datetime.min


# first_state = State(storage=BaseStorage())
# js = JsonFileStorage(file_path)
# js.retrieve_state()
my_state = State(storage=JsonFileStorage(file_path=file_path))
