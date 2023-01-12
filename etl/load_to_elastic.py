import logging

import backoff
from dotenv import dotenv_values
from elastic_transport import ConnectionError
from elasticsearch import Elasticsearch, helpers

from schemas import FilmworkSchemaOut

config = dotenv_values(".env")


class LoadElastic:
    """Класс реализует метод загрузки данных в Elasticsearch."""

    def __init__(self, es_host: str, es_user: str, es_password: str):
        self.es = Elasticsearch(
            es_host, basic_auth=(es_user, es_password),
            verify_certs=False
        )

    @staticmethod
    @backoff.on_exception(
        backoff.expo,
        ConnectionError,
        max_tries=10,
    )
    def send_data_to_es(es: Elasticsearch, es_data: list[FilmworkSchemaOut], index: str) -> tuple[int, list]:
        query = [{'_index': index, '_id': data.id, '_source': data.dict()}
                 for data in es_data]
        rows_count, errors = helpers.bulk(es, query)
        if errors:
            logging.error('Error while save data in Elasticsearch',
                          extra={'query': query, 'errors': errors})
        return rows_count, errors
