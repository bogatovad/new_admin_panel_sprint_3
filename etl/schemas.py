from datetime import datetime
from typing import Optional

from pydantic import BaseModel, BaseSettings, Field


class FilmworkSchemaIn(BaseModel):
    fw_id: str
    title: str
    description: Optional[str]
    rating: Optional[float]
    type: str
    created: datetime
    modified: datetime
    role: Optional[str]
    person_id: Optional[str]
    full_name: Optional[str]
    genre_name: str


class PersonSchema(BaseModel):
    id: str
    name: str


class FilmworkSchemaOut(BaseModel):
    id: str
    imdb_rating: Optional[float]
    genre: list[str]
    title: str
    description: Optional[str]
    director: list[str]
    actors_names: list[str]
    writers_names: list[str]
    actors: list[PersonSchema]
    writers: list[PersonSchema]


class ElasticSettings(BaseSettings):
    es_host: str = Field('http://elasticsearch:9200', env='ES_HOST')
    es_user: str = Field('', env='ES_USER')
    es_password: str = Field('', env='ES_PASSWORD')
