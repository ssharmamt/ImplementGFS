import json


class GFSMaster:

    json = {}

    def __init__(self):
        pass

    @staticmethod
    def load_json():
        with open("Namespace.json", "r") as rf:
            return  json.load(rf)

    def get_read_chunks(self, file_name, chunk_id):
        if file_name is None:
            return
        data = self.load_json()
        return data[file_name][chunk_id]["Primary"]

    def get_list_replicas(self, file_name, chunk_handle_id):
        if file_name is not None:
            data = self.load_json()
            return data[file_name][chunk_handle_id]["replicas"]
        return []
