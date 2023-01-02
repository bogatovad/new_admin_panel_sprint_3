import logging
import time

import psycopg2
from dotenv import dotenv_values
from psycopg2.extras import DictCursor

from config import dsl
from load_to_elastic import LoadElastic
from postgres_extract import PostgresExtract
from schemas import ElasticSettings, FilmworkSchemaOut
from state import JsonFileStorage, State

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")


def main():
    state = State(JsonFileStorage('state.json'))
    elastic_conn = ElasticSettings().dict()
    config = dotenv_values('.env')

    while True:
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
                            elastic_database.es, film_works_to_elastic)
                        state.set_state(key_state, last_modified.isoformat())

        except psycopg2.OperationalError:
            logging.error('Connection refused')

        time.sleep(int(config.get('SLEEP')))


if __name__ == '__main__':
    main()
