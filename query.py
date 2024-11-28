from typing import Union
from config import app


def query_part_for_where(data: Union[list, tuple]) -> str:
    """Query constructor for WHERE operator"""
    return f"{tuple(data)}" if len(data) > 1 else f"= '{data[0]}'"


def query_film_work(modified_value):
    return f"""
SELECT id, modified
FROM content.film_work
WHERE modified > '{modified_value}'
ORDER BY modified
LIMIT {app.limit}
    """


def query_genre(modified_value):
    return f"""
SELECT id, modified
FROM content.genre
WHERE modified > '{modified_value}'
ORDER BY modified
LIMIT {app.limit}
    """


def query_person(modified_value):
    return f"""
SELECT id, modified
FROM content.person
WHERE modified > '{modified_value}'
ORDER BY modified
LIMIT {app.limit}
    """


def query_genre_film_work(genre_id):
    return f"""
SELECT fw.id, fw.modified
FROM content.film_work fw
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
WHERE gfw.genre_id IN {query_part_for_where(genre_id)}
ORDER BY fw.modified
LIMIT {app.limit}
    """


def query_person_film_work(person_id):
    return f"""
SELECT fw.id, fw.modified
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
WHERE pfw.person_id IN {query_part_for_where(person_id)}
ORDER BY fw.modified
LIMIT {app.limit}
    """


def query_film_works_id(film_work_id: tuple) -> str:
    return f"""
        SELECT
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.created,
            fw.modified,
            pfw.role,
            p.id,
            p.full_name,
            g.name
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id IN {query_part_for_where(film_work_id)};
    """
