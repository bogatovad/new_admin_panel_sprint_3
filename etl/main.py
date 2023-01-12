import logging
import time

import psycopg2
from dotenv import dotenv_values
from psycopg2.extras import DictCursor

from config import dsl
from load_to_elastic import LoadElastic
from postgres_extract import PostgresExtract
from schemas import ElasticSettings, FilmworkSchemaOut, GenreSchemaOut, PersonSchemaOut
from state import JsonFileStorage, State

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")


def process_etl_movies(elastic_conn, state):
    try:
        with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
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
                        logging.info('Обновленных данных нет')
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

    except psycopg2.OperationalError:
        logging.error('Connection refused')


def process_etl_genre(elastic_conn, state):
    try:
        with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
            postgres_extract = PostgresExtract(pg_conn=pg_conn)
            elastic_database = LoadElastic(**elastic_conn)
            genres = postgres_extract.get_modified_genres('2021-01-01 17:54:49.93152+00')
            persons = postgres_extract.get_modified_persons('2021-01-01 17:54:49.93152+00')

            genres_to_elastic = [GenreSchemaOut(**genre) for genre in genres]
            persons_to_elastic = [PersonSchemaOut(**person) for person in persons]

            elastic_database.send_data_to_es(
                elastic_database.es, genres_to_elastic, 'genres')

            elastic_database.send_data_to_es(
                elastic_database.es, persons_to_elastic, 'persons')
    except psycopg2.OperationalError:
        logging.error('Connection refused')


def process_etl_persons():
    pass


def main():
    state = State(JsonFileStorage('state.json'))
    elastic_conn = ElasticSettings().dict()
    config = dotenv_values('.env')

    while True:
        process_etl_genre(elastic_conn, state)
        time.sleep(int(config.get('SLEEP')))


if __name__ == '__main__':
    main()
