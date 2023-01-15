import logging
import sqlite3
import sys
from contextlib import contextmanager

import orjson

from models.models import table_to_schema
from psycopg2.extensions import connection as _connection

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")


@contextmanager
def cursor_manager(conn):
    curs = conn.cursor()
    yield curs
    curs.close()


class CreateSqlQuery:
    """The class implements helper methods for building queries."""

    @staticmethod
    def create_template(fields: list[str]) -> str:
        """Create template for mogrify."""
        template: str = ", ".join(["%s"] * len(fields))
        return f"({template})"

    @staticmethod
    def create_values(fields: list[str]) -> str:
        """Create part of string SQL query."""
        values: str = ', '.join(fields)
        return f'({values})'

    @staticmethod
    def get_values(model):
        return list(orjson.loads(model.json()).values())


class PostgresSaver(CreateSqlQuery):
    def __init__(self, pg_conn: _connection):
        self.pg_conn = pg_conn
        self.schema_name = 'content'

    def get_conn(self):
        return self.pg_conn

    def create_schema(self):
        logging.info('Creating schema.')
        with cursor_manager(self.pg_conn) as cursor:
            sql_code_create_schema = open("schema_design/movies_database.ddl", "r").read()
            try:
                cursor.execute(sql_code_create_schema)
            except Exception as ex:
                logging.info(f'Execption {ex}')

    def save_all_data(self, current_table, dict_values):
        """Save data."""
        current_model = table_to_schema[current_table]
        fields = current_model.get_fields()
        template = self.create_template(fields)
        values = self.create_values(fields)

        with cursor_manager(self.pg_conn) as cursor:
            try:
                args = ','.join(cursor.mogrify(template, self.get_values(
                    item)).decode() for item in dict_values)
                cursor.execute(f"INSERT INTO {self.schema_name}.{current_table} {values} "
                               f"VALUES {args} ON CONFLICT (id) DO NOTHING RETURNING id;")
                last_id_row = cursor.fetchall()
                if last_id_row:
                    self.pg_conn.commit()
            except Exception as ex:
                sys.exit(f"Exception while inserting data to postgres. {ex}")


class SQLiteExtractor:
    def __init__(self, sqlite_conn: sqlite3.Connection):
        self.sqlite_conn = sqlite_conn
        self.schema_name = None

    def get_conn(self):
        return self.sqlite_conn

    @contextmanager
    def conn_context(self):
        conn = self.sqlite_conn
        conn.row_factory = sqlite3.Row
        yield conn

    def extract_data(self, postgres_saver):
        """Get all data from sqlite."""
        with self.conn_context() as conn:
            count_rows: int = 1000
            tables: list[str] = ["genre", "person", "film_work",
                                 "genre_film_work", "person_film_work"]

            with cursor_manager(conn) as curs:
                for table in tables:
                    try:
                        curs.execute(
                            f'SELECT * FROM {table};')
                    except Exception as ex:
                        logging.info(f'Exception {ex}')
                    while True:
                        three_rows = curs.fetchmany(count_rows)
                        if three_rows:
                            model_schema = table_to_schema.get(table)
                            yield table, (model_schema(**item) for item in three_rows)
                        else:
                            break


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Main method to download data from SQLite to Postgres."""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)
    data_generator = sqlite_extractor.extract_data(postgres_saver)

    # создаем схему из ddl файла.
    postgres_saver.create_schema()

    for table, values in data_generator:
        logging.info(f'Запись данных в {table}')
        postgres_saver.save_all_data(table, values)
