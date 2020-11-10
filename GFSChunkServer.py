import os


class GFSChunkServer:

    def __init__(self):
        pass

    def read_from_chunk(chunk_id_path):

        if chunk_id_path is not None:
            fo = open("SampleNamespace/"+chunk_id_path, "r")
            data = fo.read()
            fo.close()
            return data
        return

    def write_chunk(chunk_id_path, data):
        chunk_id_path = "SampleNamespace/"+chunk_id_path
        if chunk_id_path is not None:
            fo = open(chunk_id_path, "a")
            if os.path.getsize(chunk_id_path) + len(data.encode("utf-8")) > 64000000:
                print("out of space create a new chunk")
            else:
                fo.write(data)
            fo.close()

        else:
            print("path you want to write to cannot be null")