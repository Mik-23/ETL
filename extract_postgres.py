import logging
import psycopg2
from backoff import backoff
from config import postgres, app
from typing import Dict, List, Optional
from psycopg2 import OperationalError
from psycopg2.extras import DictCursor
from psycopg2.extensions import connection as pg_connection
from postgres_table import (PersonType, FilmWorkSchema, GenreSchema,
                            PersonSchema, GenreFilmWorkSchema,
                            PersonFilmWorkSchema, ESFilmWorkSchema)
from query import (query_film_work, query_genre,
                   query_person, query_genre_film_work,
                   query_person_film_work, query_film_works_id)


logger = logging.getLogger(__name__)
con: str = "PG connection"
create: str = "Create cursor"


class PostgresConnector:
    # Класс для подключения к БД

    def __init__(self, pg_settings=postgres):
        self.dsl: dict = pg_settings.dsl
        self.conn: Optional[pg_connection] = None

    @property
    def connection(self):
        # Проверить соединение, подключиться если соединение закрыто
        if self.conn and not self.conn.closed:
            conn = self.conn
        else:
            conn = self.create_conn()
        return conn

    @backoff(exceptions=[OperationalError], logger=logger, title=con)
    def create_conn(self) -> pg_connection:
        # Подключение к PostgreSQL
        return psycopg2.connect(**self.dsl, cursor_factory=DictCursor)


class PostgresCursor:
    def __init__(self, connection):
        self._cursor: Optional[DictCursor] = None
        self._connection = connection

    @property
    def cursor(self) -> DictCursor:
        # Проверить курсор, создать, если он закрыт
        if self._cursor and not self._cursor.closed:
            cur = self._cursor
        else:
            cur = self.create_cur()
        return cur

    @backoff(exceptions=[OperationalError], logger=logger, title=create)
    def create_cur(self) -> DictCursor:
        # Создание курсора
        return self._connection.cursor()


class ExtractPostgres:
    def __init__(self, cursor, state):
        self.cursor = cursor
        self.state = state

    def query_executor(self, query):
        # Выполнить запрос по таблицам PostreSQL
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def select_first_part(self, table_name) -> tuple:
        # Забираем данные из таблиц с фильмами, жанрами и участниками

        # Инициализируем фильмы
        key_film = f"film_work_{app.state_field}"
        modified_film = self.state.get_state(key_film)
        film_work = self.query_executor(query_film_work(modified_film))
        film_work_id = [FilmWorkSchema(**fw).id for fw in film_work]
        film_work_mod = str(film_work[-1].get(app.state_field))

        # Инициализируем жанры
        key_genre = f"genre_{app.state_field}"
        modified_genre = self.state.get_state(key_genre)
        genre = self.query_executor(query_genre(modified_genre))
        genre_id = [GenreSchema(**g).id for g in genre]
        genre_mod = str(genre[-1].get(app.state_field))

        # Инициализируем участников
        key_person = f"person_{app.state_field}"
        modified_person = self.state.get_state(key_person)
        person = self.query_executor(query_person(modified_person))
        person_id = [PersonSchema(**p).id for p in person]
        person_mod = str(person[-1].get(app.state_field))
        if table_name == 'film_work':
            if film_work:
                return (film_work_id, film_work_mod)
            else:
                return [], None
        elif table_name == 'genre':
            if genre:
                return (genre_id, genre_mod)
            else:
                return [], None
        elif table_name == 'person':
            if person:
                return (person_id, person_mod)
            else:
                return [], None

    def select_genre_film_work(self, genre_id) -> List[str]:
        # Забираем данные из таблицы жанров фильмов
        genre_film_work = self.query_executor(query_genre_film_work(genre_id))
        data_genre_film_work = [
            GenreFilmWorkSchema(**gfw).id
            for gfw in genre_film_work
        ]
        if genre_film_work:
            return data_genre_film_work

    def select_person_film_work(self, person_id) -> List[str]:
        # Забираем данные из таблицы с участниками фильмов
        query = query_person_film_work(person_id)
        person_film_work = self.query_executor(query)
        data_person_film_work = [
            PersonFilmWorkSchema(**pfw).id
            for pfw in person_film_work]
        if person_film_work:
            return data_person_film_work

    def select_film_work_id(self, film_work_id):
        # Создаём функцию для переноса фильмов в ES
        film_works = self.query_executor(query_film_works_id(film_work_id))
        all_data = {}
        for fw in film_works:
            # Объединяем повторяющиеся колонки в таблице
            genres: Optional[List[str]] = []
            directors_names: Optional[List[str]] = []
            actors_names: Optional[List[str]] = []
            writers_names: Optional[List[str]] = []
            directors: Optional[List[Dict[str, str]]] = []
            actors: Optional[List[Dict[str, str]]] = []
            writers: Optional[List[Dict[str, str]]] = []

            # Находим неповторяющиеся значения по ключу
            imdb_rating = fw.get("rating")
            title = fw.get("title")
            description = fw.get("description")
            fw_id = fw['fw_id']

            if fw_id not in all_data:
                # Создаём словарь для хранения данных
                all_data[fw_id] = {
                    "id": fw.get("fw_id"),
                    "imdb_rating": imdb_rating,
                    "genres": genres,
                    "title": title,
                    "description": description,
                    "directors_names": directors_names,
                    "actors_names": actors_names,
                    "writers_names": writers_names,
                    "directors": directors,
                    "actors": actors,
                    "writers": writers,
                }

            # Группируем жанры по фильмам
            genre_name = fw.get("name")
            if genre_name and genre_name not in all_data[fw_id]["genres"]:
                all_data[fw_id]["genres"].append(genre_name)

            # Находим id и имена участников
            person_name = fw.get("full_name")
            person_id = fw.get("id")
            person = {"id": person_id, "name": person_name}
            directors_list = [d["id"] for d in all_data[fw_id]["directors"]]
            actors_list = [a["id"] for a in all_data[fw_id]["actors"]]
            writers_list = [w["id"] for w in all_data[fw_id]["writers"]]

            # Группируем участников по режиссёрам, актёрам и писателям
            if fw.get("role") == PersonType.director:
                if person_id not in directors_list:
                    all_data[fw_id]["directors"].append(person)
                    all_data[fw_id]["directors_names"].append(person_name)

            elif fw.get("role") == PersonType.actor:
                if person_id not in actors_list:
                    all_data[fw_id]["actors"].append(person)
                    all_data[fw_id]["actors_names"].append(person_name)

            elif fw.get("role") == PersonType.writer:
                if person_id not in writers_list:
                    all_data[fw_id]["writers"].append(person)
                    all_data[fw_id]["writers_names"].append(person_name)

        # Создаём генератор словарей для передачи в ES
        for film in all_data.values():
            yield ESFilmWorkSchema(**film).dict()
# print(load.connect_and_load_postgres())
