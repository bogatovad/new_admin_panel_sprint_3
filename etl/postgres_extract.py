import logging

from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from utils import backoff


class PostgresExtract:
    """Класс реализует методы доступа к postgres."""

    LIMIT = 500

    def __init__(self, pg_conn: _connection):
        self.pg_conn = pg_conn
        self.curs = pg_conn.cursor()

    @staticmethod
    @backoff()
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
            f'LIMIT {self.LIMIT} OFFSET {offset}'
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
        query: str = (
            "SELECT "
            "fw.id as fw_id, "
            "fw.title, "
            "fw.description, "
            "fw.rating, "
            "fw.type, "
            "fw.created, "
            "fw.modified, "
            "pfw.role, "
            "p.id as person_id, "
            "p.full_name, "
            "g.name as genre_name "
            "FROM content.film_work fw "
            "LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id "
            "LEFT JOIN content.person p ON p.id = pfw.person_id "
            "LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id "
            "LEFT JOIN content.genre g ON g.id = gfw.genre_id "
            f"WHERE fw.id IN ({ids}); "
        )
        return self.extract_data(query, self.curs)