from typing import List, Optional, Dict
from enum import Enum
from uuid import UUID

from datetime import datetime
from pydantic import BaseModel


class PersonType(str, Enum):
    actor = "actor"
    director = "director"
    writer = "writer"


class FilmType(str, Enum):
    movie = "movie"
    tv_show = "tv_show"


class IDModel(BaseModel):
    id: UUID


class ModifiedModel(BaseModel):
    modified: datetime


class FilmWorkSchema(IDModel, ModifiedModel):
    pass


class GenreSchema(IDModel, ModifiedModel):
    pass


class GenreFilmWorkSchema(IDModel, ModifiedModel):
    pass


class PersonSchema(IDModel, ModifiedModel):
    pass


class PersonFilmWorkSchema(IDModel, ModifiedModel):
    pass


class ESFilmWorkSchema(IDModel):

    imdb_rating: Optional[float] = None
    genres: Optional[List[str]] = None
    title: str
    description: Optional[str] = None
    directors_names: Optional[List[str]] = None
    actors_names: Optional[List[str]] = None
    writers_names: Optional[List[str]] = None
    directors: Optional[List[Dict[str, str]]] = None
    actors: Optional[List[Dict[str, str]]] = None
    writers: Optional[List[Dict[str, str]]] = None
