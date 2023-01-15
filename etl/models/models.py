import uuid
from enum import Enum
from typing import Optional, Type

from pydantic import BaseModel, Field


class GetFieldsMixin(BaseModel):
    id: uuid.UUID

    @classmethod
    def get_fields(cls):
        return list(cls.__fields__)


class Filmwork(GetFieldsMixin):
    title: str
    description: Optional[str]
    type: str
    rating: Optional[float] = Field(default=0.0)


class Genre(GetFieldsMixin):
    name: str
    description: Optional[str]


class Person(GetFieldsMixin):
    full_name: str


class GenreFilmWork(GetFieldsMixin):
    genre_id: uuid.UUID
    film_work_id: uuid.UUID


class PersonFilmWork(GetFieldsMixin):
    class Role(Enum):
        actor = "actor"
        director = "director"
        writer = "writer"

    person_id: uuid.UUID
    film_work_id: uuid.UUID
    role: Role


table_to_schema: dict[str, Type[BaseModel]] = {
    'film_work': Filmwork,
    'genre': Genre,
    'person': Person,
    'genre_film_work': GenreFilmWork,
    'person_film_work': PersonFilmWork
}