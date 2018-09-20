import ConfigParser
import os
from elasticsearch import Elasticsearch


class ConfigOperations(object):
    def __init__(self):
        config = ConfigParser.ConfigParser()
        config.read("config.ini")
        self.port = int(config.get("ES", "port"))
        self.host = config.get("ES", "host")
        self.index = config.get("ES", "index")
        self.filePath = config.get("LOGGING", "filePath")
        self.interval = config.get("EXECUTION", "sleeptime")


class EsOperations(object):
    def __init__(self):
        self.es = None

    def connect(self, host, port):
        try:
            self.es = Elasticsearch([{'host': host, 'port': port}], sniff_on_start=True)
            return True
        except Exception as ex:
            return False

    def get_data(self, index, query):
        try:
            res = self.es.search(index=index, body=query)
            return res
        except Exception as ex:
            raise ex

    def update_record(self, index, doc_type, doc_id):
        try:
            return self.es.update(index=index, doc_type=doc_type, id=doc_id, body='{"doc" :{"cached" : false}}')
        except Exception as ex:
            raise ex


class OsOperations(object):
    @staticmethod
    def delete_directory_if_empty(directory):
        try:
            os.rmdir(directory)
        except OSError as ex:
            raise ex

    def purge_directory(self, dir_to_purge):
        try:
            for dir_file in os.listdir(dir_to_purge):
                file_path = os.path.join(dir_to_purge, dir_file)
                if os.path.isfile(file_path):
                    self.delete_file(file_path)
            self.delete_directory_if_empty(dir_to_purge)
        except Exception as ex:
            raise ex

    @staticmethod
    def delete_file(file_2_delete):
        try:
            os.remove(file_2_delete)
            return True
        except Exception as ex:
            raise ex
