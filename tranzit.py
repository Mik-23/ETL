import logging
from config import elastic, app
from time import sleep
from extract_postgres import ExtractPostgres, PostgresConnector, PostgresCursor
from load_elastic import LoadElastic
from state import my_state
from delete_json_file import delete_modified

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
load = LoadElastic(elastic, 'movies')


def load_data(extract, film_work_id: tuple) -> None:
    if film_work_id:
        actions: list = []
        for film_work in extract.select_film_work_id(film_work_id):
            actions.append(film_work)
            # print(actions)
            if len(actions) == app.limit:
                load.elastic_connect_and_load(actions)
                actions.clear()
        else:
            if actions:
                load.elastic_connect_and_load(actions)


def etl(pg_cur):
    # Очищаем старое хранилище
    delete_modified()
    # load.create_index(400)
    extract = ExtractPostgres(pg_cur, my_state)

    # Переносим данные из таблиц с участниками и участниками фильмов
    person_id, person_modified = extract.select_first_part('person')
    person_film_work = extract.select_person_film_work(person_id)

    # Переносим данные из таблиц с жанрами и фильмов с этими жанрами
    genre_id, genre_modified = extract.select_first_part('genre')
    genre_film_work = extract.select_genre_film_work(genre_id)

    # Переносим данные из таблицы с фильмами
    film_work_id, film_work_modified = extract.select_first_part('film_work')
    all_data = set(film_work_id + genre_film_work + person_film_work)
    print('Данные из Postgres собраны')
    load_data(extract, tuple(all_data))
    # print(genre_modified)

    # Записываем новое состояние в хранилище
    if person_modified:
        my_state.set_state(key="person_modified", value=person_modified)
    if genre_modified:
        my_state.set_state(key="genre_modified", value=genre_modified)
    if film_work_modified:
        my_state.set_state(key="film_work_modified", value=film_work_modified)


if __name__ == '__main__':
    logger.info("Начало процесса")
    try:
        with PostgresConnector().connection as postgres_conn:
            with PostgresCursor(postgres_conn).cursor as postgres_cur:
                # load.delete_index(400)
                etl(postgres_cur)
                print('Успешно!')
                print('Теперь данные есть в Elasticsearch')
    except Exception as e:
        logger.error(f'Ошибка при передачи данных: {e}')
    else:
        logger.info("Закрыть соединение")
        load.connect.transport.close()
        if not postgres_cur.closed:
            postgres_cur.close()
        if not postgres_conn.closed:
            postgres_conn.close()
    logger.info(f"Задержка составила {app.delay} секунд")
    sleep(app.delay)
