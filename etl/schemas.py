from typing import Optional

from pydantic import BaseModel, BaseSettings, Field, validator


class PersonSchema(BaseModel):
    id: str
    name: str


class FilmworkSchemaOut(BaseModel):
    id: str
    imdb_rating: Optional[float]
    genre: list[str]
    title: str
    description: Optional[str]
    director: Optional[list[str]]
    actors_names: Optional[list[str]]
    writers_names: Optional[list[str]]
    actors: Optional[list[PersonSchema]]
    writers: Optional[list[PersonSchema]]

    @validator('director')
    def validate_director(cls, value):
        return [] if value is None else value


class ElasticSettings(BaseSettings):
    es_host: str = Field('http://elasticsearch:9200', env='ES_HOST')
    es_user: str = Field('', env='ES_USER')
    es_password: str = Field('', env='ES_PASSWORD')
