import logging
import json
import time
from typing import Optional
from http import HTTPStatus
from elasticsearch import (Elasticsearch, helpers,
                           SSLError, ConnectionError, TransportError)
from config import elastic
from backoff import backoff


logger = logging.getLogger(__name__)
status = HTTPStatus.BAD_REQUEST
es_con: str = "ES connection"
error_list = [SSLError, ConnectionError, TransportError]


class LoadElastic:

    def __init__(self, config=elastic, index: str = 'movies'):
        self.config = config
        self.index = index
        self.file_path = config.file_path
        self.conn: Optional[Elasticsearch] = None

    @backoff(exceptions=error_list, logger=logger, title=es_con)
    def create_connect(self) -> Elasticsearch:
        return Elasticsearch(
             hosts=[f"http://{self.config.host}:{self.config.port}"])

    @property
    def connect(self):
        if self.conn and self.conn.ping():
            conn = self.conn
        else:
            conn = self.create_connect()
        return conn

    def create_index(self, http_res: int = status) -> None:
        if not self.connect.indices.exists(index=self.index):
            with open(self.file_path, "r") as es_file:
                index_settings = json.load(es_file)
                try:
                    self.connect.indices.create(index=self.index, body=index_settings, ignore=http_res)
                    print('Индекс создан')
                except Exception as e:
                    print(f"Произошла ошибка {e}")
        else:
            print(f"Индекс {self.index} уже существует.")

    def delete_index(self, ignore_http_response: int = status) -> None:
        try:
            self.connect.indices.delete(
                    index=self.index, ignore=ignore_http_response)
            print('Индекс удалён')
        except Exception as e:
            print(f"Произошла ошибка {e}")

    def elastic_connect_and_load(self, data):
        start = time.perf_counter()
        # print(data)
        if not self.index:
            raise ValueError("Индекс должен быть создан.")
        data_gen = [{'_index': self.index,
                     "_id": elem.get('id'),
                     "_source": {**elem}}
                    for elem in data]
        try:
            lines, status = helpers.bulk(
                client=self.connect,
                actions=data_gen, chunk_size=100)
            all_time = time.perf_counter() - start
            print(f"{lines} документов успешно проиндексировано.")
            print(f"Длительность передачи данных {all_time}")
        except helpers.BulkIndexError as e:
            print(f"{len(e.errors)} документов не проиндексировались.")
            for error in e.errors:
                print("Ошибка индексации:", error)
                print("Неверный документ:", error['index']['_source'])
