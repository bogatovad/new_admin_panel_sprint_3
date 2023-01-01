from psycopg2.extras import DictCursor
import psycopg2
import logging
import time

from config import dsl
from load_to_elastic import LoadElastic
from postgres_extract import PostgresExtract
from schemas import ElasticSettings, FilmworkSchemaIn, FilmworkSchemaOut
from state import JsonFileStorage, State
from utils import TransferDataError, create_array, add_item_func

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")


def add_persons(film_data) -> tuple[dict, dict, dict, set]:
    """Собираем информацию о персоналиях."""
    actors = {}
    writers = {}
    directors = {}
    genres = set()
    action = {
        'actor': (add_item_func, actors),
        'writer': (add_item_func, writers),
        'director': (add_item_func, directors)
    }

    for row in film_data:
        genres.add(row.genre_name)

        if row.role:
            func, d = action[row.role]
            func(row.person_id, row.full_name, d)

    return actors, writers, directors, genres


def transform_data(film_data: list[FilmworkSchemaIn], film_id: str) -> FilmworkSchemaOut:
    """Преобразование данных в формат Elasticsearch."""
    actors, writers, directors, genres = add_persons(film_data)
    film = film_data[0]
    return FilmworkSchemaOut(
        id=film_id,
        imdb_rating=film.rating,
        genre=list(genres),
        title=film.title,
        description=film.description,
        director=list(directors.values()),
        actors_names=list(actors.values()),
        writers_names=list(writers.values()),
        actors=create_array(actors),
        writers=create_array(writers),
    )


def main():
    state = State(JsonFileStorage('state.json'))
    elastic_conn = ElasticSettings().dict()

    while True:
        try:
            with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
                postgres_extract = PostgresExtract(pg_conn=pg_conn)
                elastic_database = LoadElastic(**elastic_conn)
                tables = ['film_work', 'genre', 'person']

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
                        film_work_data = [
                            FilmworkSchemaIn.parse_obj(item)
                            for item in data
                        ]

                        film_works_to_elastic = []
                        for film_id in ids_film_work:
                            film_data = list(
                                filter(lambda x: x.fw_id == film_id, film_work_data))
                            film_works_to_elastic.append(
                                transform_data(film_data, film_id))

                        logging.info(
                            f"Загружаем данные {film_works_to_elastic}")
                        elastic_database.send_data_to_es(
                            elastic_database.es, film_works_to_elastic)
                        state.set_state(key_state, last_modified.isoformat())

        except psycopg2.OperationalError:
            logging.error('Connection refused')

        except TransferDataError:
            logging.error('Error while sending data to elasticsearch')

        time.sleep(1)


if __name__ == '__main__':
    main()
