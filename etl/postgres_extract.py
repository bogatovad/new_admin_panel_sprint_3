import logging

import backoff
from elastic_transport import ConnectionError
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor


class PostgresExtract:
    """Класс реализует методы доступа к postgres."""

    LIMIT = 500

    def __init__(self, pg_conn: _connection):
        self.pg_conn = pg_conn
        self.curs = pg_conn.cursor()

    def __del__(self):
        self.curs.close()
        logging.info('Cursor is close')

    @staticmethod
    @backoff.on_exception(
        backoff.expo,
        ConnectionError,
        max_tries=10,
    )
    def extract_data(query: str, curs: DictCursor) -> list:
        curs.execute(query)
        data = curs.fetchall()
        return data

    def get_ids_modified_data(self, table: str,  modified: str, offset: int):
        """Получить обновленные данные."""
        query: str = (
            "SELECT id, modified "
            f"FROM content.{table} "
            f"WHERE modified > '{modified}' "
            "ORDER BY modified "
            f"LIMIT {self.LIMIT} OFFSET {offset}"
        )
        logging.info(f'{modified=}-{table=}')
        data = self.extract_data(query, self.curs)
        if not data:
            return '', []
        last_modified = data[-1][1]
        return last_modified, [data[0] for data in data]

    def get_ids_film_work_by_person(self, ids_person: list[str]):
        """Получить связанные фильмы из обновлений в персонах."""
        ids = str(ids_person)[1:-1]
        query: str = (
            "SELECT fw.id "
            "FROM content.film_work fw "
            "LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id "
            f"WHERE pfw.person_id IN ({ids}) "
            "ORDER BY fw.modified "
        )
        return tuple(data[0] for data in self.extract_data(query, self.curs))

    def get_ids_film_work_by_genre(self, ids_genre: list[str]):
        """Получить связанные фильмы из обновлений в жанрах."""
        ids = str(ids_genre)[1:-1]
        query: str = (
            "SELECT fw.id "
            "FROM content.film_work fw "
            "LEFT JOIN content.genre_film_work pfw ON pfw.film_work_id = fw.id "
            f"WHERE pfw.genre_id IN ({ids}) "
            "ORDER BY fw.modified "
        )
        return tuple(data[0] for data in self.extract_data(query, self.curs))

    def get_ids_data_modified(self, table, ids):
        return {
            'person': self.get_ids_film_work_by_person,
            'genre': self.get_ids_film_work_by_genre,
            'film_work': lambda _ids: ids,
        }[table](ids)

    def get_all_data_film_work(self, ids_film_work: list[str]):
        """Получить всю информацию о фильме."""
        ids = str(ids_film_work)[1:-1]
        query = (f"""
        SELECT fw.id AS id,
            fw.rating AS imdb_rating,
            fw.title,
            fw.description,
            fw.created,
            fw.modified,
            array_agg(DISTINCT g.name) AS genre,
            array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'actor') AS actors_names,
            array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'writer') AS writers_names,
            array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'director') AS director,
            COALESCE(json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'actor'),'[]') AS actors,
            COALESCE(json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'writer'), '[]') AS writers
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN ({ids})
            GROUP BY fw.id
            ORDER BY fw.modified;
        """)
        return self.extract_data(query, self.curs)

    def get_modified_genres(self, modified: str):
        query = (
            "SELECT g.id, g.name, g.description, g.modified FROM content.genre g "
            f"WHERE modified > '{modified}' ORDER BY g.modified;"
        )
        return self.extract_data(query, self.curs)

    def get_modified_persons(self, modified: str):
        query = (
            "SELECT p.id, p.full_name, p.modified FROM content.person p "
            f"WHERE modified > '{modified}' ORDER BY p.modified;"
        )
        return self.extract_data(query, self.curs)
