import logging
import sqlite3
import time

import backoff
from elastic_transport import ConnectionError
import psycopg2
from dotenv import dotenv_values
from psycopg2.extras import DictCursor

from config import dsl
from sqlite_to_postgres import load_from_sqlite
from indexes import index_to_schema
from load_to_elastic import LoadElastic
from postgres_extract import PostgresExtract
from schemas import ElasticSettings, FilmworkSchemaOut, GenreSchemaOut, PersonSchemaOut
from state import JsonFileStorage, State

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")


def connect_to_database(process_etl_func):
    def wrapper(elastic_conn, state, pg_conn=None):
        try:
            with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
                process_etl_func(elastic_conn, state, pg_conn)
        except psycopg2.OperationalError:
            logging.error('Connection refused')
    return wrapper


@connect_to_database
def process_etl_movies(elastic_conn, state, pg_conn=None):
    """Загрузка данных по фильмам в elasticsearch."""
    postgres_extract = PostgresExtract(pg_conn=pg_conn)
    elastic_database = LoadElastic(**elastic_conn)
    tables = ('film_work', 'genre', 'person')

    for table in tables:
        key_state = f'{table}_modified'
        last_modified_state = state.get_state(key_state)
        offset = 0

        while True:
            logging.info('Read data from postgres')
            last_modified, ids_modified = postgres_extract.get_ids_modified_data(
                table, last_modified_state, offset)
            offset += postgres_extract.LIMIT
            if not ids_modified:
                logging.info('Обновленных данных по фильмам нет.')
                break
            ids_film_work = postgres_extract.get_ids_data_modified(
                table, ids_modified)
            data = postgres_extract.get_all_data_film_work(
                ids_film_work)
            film_works_to_elastic = [
                FilmworkSchemaOut(**item) for item in data]
            elastic_database.send_data_to_es(
                elastic_database.es, film_works_to_elastic, 'movies')
            state.set_state(key_state, last_modified.isoformat())


@connect_to_database
def process_etl_genres(elastic_conn, state, pg_conn=None):
    """Загрузка данных по жанрам в elasticsearch."""
    postgres_extract = PostgresExtract(pg_conn=pg_conn)
    elastic_database = LoadElastic(**elastic_conn)
    key_state = 'genres_table'
    last_modified_state = state.get_state(key_state)
    last_modified_genres, genres = postgres_extract.get_modified_genres(last_modified_state)
    if not genres:
        logging.info('Обновленных данных по жанрам нет.')
        return
    genres_to_elastic = [GenreSchemaOut(**genre) for genre in genres]
    elastic_database.send_data_to_es(
        elastic_database.es, genres_to_elastic, 'genres')
    state.set_state(key_state, last_modified_genres.isoformat())


@connect_to_database
def process_etl_persons(elastic_conn, state, pg_conn=None):
    """Загрузка данных по персонам в elasticsearch."""
    postgres_extract = PostgresExtract(pg_conn=pg_conn)
    elastic_database = LoadElastic(**elastic_conn)
    key_state = 'persons_table'
    last_modified_state = state.get_state(key_state)
    last_modified_persons, persons = postgres_extract.get_modified_persons(last_modified_state)
    if not persons:
        logging.info('Обновленных данных по персонам нет.')
        return
    persons_to_elastic = [PersonSchemaOut(**person) for person in persons]
    elastic_database.send_data_to_es(
        elastic_database.es, persons_to_elastic, 'persons')
    state.set_state(key_state, last_modified_persons.isoformat())


@backoff.on_exception(
    backoff.expo,
    ConnectionError,
    max_tries=50,
)
def create_indexes(elastic_conn):
    """Создает индексы если их нет."""
    elastic_database = LoadElastic(**elastic_conn)

    for index in ("movies", "genres", "persons"):
        if elastic_database.es.indices.exists(index=index):
            logging.info(f'Index {index} already exist.')
        else:
            data_create_index = {
                "index": index,
                "ignore": 400,
                "body": index_to_schema.get(index)
            }
            elastic_database.es.indices.create(
               **data_create_index
            )


def main():
    state = State(JsonFileStorage('state.json'))
    elastic_conn = ElasticSettings().dict()
    config = dotenv_values('../enviroments/.env')
    create_indexes(elastic_conn)

    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)

    while True:
        process_etl_movies(elastic_conn, state)
        process_etl_genres(elastic_conn, state)
        process_etl_persons(elastic_conn, state)
        time.sleep(int(config.get('SLEEP')))


if __name__ == '__main__':
    main()
