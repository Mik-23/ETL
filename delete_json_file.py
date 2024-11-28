import json
import psycopg2
from config import postgres


def delete_modified():
    # Удаляет дату обновления для избежания ошибки
    conn = psycopg2.connect(**postgres.dsl)
    cur = conn.cursor()
    cur.execute("SELECT modified FROM content.genre")
    genre = cur.fetchall()
    cur.execute("SELECT modified FROM content.film_work")
    film_work = cur.fetchall()
    cur.execute("SELECT modified FROM content.person")
    person = cur.fetchall()
    with open("state.json", "r") as f:
        file = f.read()
        if file != '':
            if len(json.loads(file)) == 3:
                json_file = json.loads(file)
                new_genre = []
                new_film_work = []
                new_person = []
                format_date = '%Y-%m-%d %H:%M:%S.%f+03:00'
                for tup in genre:
                    for g in tup:
                        new_genre.append(g.strftime(format_date))
                for tup in film_work:
                    for fw in tup:
                        new_film_work.append(fw.strftime(format_date))
                for tup in person:
                    for p in tup:
                        new_person.append(p.strftime(format_date))
                if max(new_genre) == json_file["genre_modified"]:
                    del json_file["genre_modified"]
                    with open("state.json", "w") as f2:
                        json.dump(json_file, f2)
                        print('Дата обновления жанра успешно удалёна')
                if max(new_film_work) == json_file["film_work_modified"]:
                    del json_file["film_work_modified"]
                    with open("state.json", "w") as f2:
                        json.dump(json_file, f2)
                        print('Дата обновления фильма успешно удалёна')
                if max(new_person) == json_file["person_modified"]:
                    del json_file["person_modified"]
                    with open("state.json", "w") as f2:
                        json.dump(json_file, f2)
                        print('Дата обновления участника успешно удалёна')
                else:
                    pass
            else:
                pass
        else:
            pass
