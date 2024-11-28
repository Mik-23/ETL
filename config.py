from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings


load_dotenv()


keys = ['dbname', 'user', 'password', 'host', 'port']
values = ['movies_database', 'postgres', '123qwe', 'localhost', 5432]
dict_dsl = dict(zip(keys, values))


class PostgresConfig(BaseSettings):
    dsl: dict = Field(dict_dsl)

    class Config:
        env_file = ".env"


class ElasticConfig(BaseSettings):
    host: str = Field("localhost")
    port: int = Field(9200)
    file_path: str = Field("es_schema.json")

    class Config:
        env_file = ".env"


class AppConfig(BaseSettings):
    limit: int = Field(100)
    delay: float = Field(3)
    state_field: str = Field('modified')
    state_file_name: str = Field('\\'+'state.json')

    class Config:
        env_file = ".env"


postgres = PostgresConfig()

elastic = ElasticConfig()

app = AppConfig()
