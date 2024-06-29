import os

from utils.model.log import WriteLine, WriteMetadataLine, BeginLine, CommitLine


class LogManager():
    BASE_DIRECTORY = '/clients'

    def __init__(self, client_id):
        self.client_id = client_id

        self.log_file = LogManager.BASE_DIRECTORY + '/' + str(client_id) + '/log'
        if not os.path.exists(self.log_file):
            open(self.log_file, 'wb').close()

        self.booleans = []
        self.integers = []
        self.changes = {}

    def begin(self, chunk_id, worker_id=None):
        with open(self.log_file, "wb") as log_file:
            begin_line = BeginLine(chunk_id, worker_id)
            log_file.write(begin_line.encode())

    def hold_change(self, k, v_old, v_new):
        if k not in self.changes:
            self.changes[k] = [v_old, v_new]
        else:
            self.changes[k][1] = v_new

    def log_changes(self):
        with open(self.log_file, "ab") as log_file:
            for k in self.changes:
                old = self.changes[k][0]
                write_line = WriteLine(k, old)
                log_file.write(write_line.encode())

        self.changes = {}

    def meta_decoder(self, k, _b):
        if k in self.booleans:
            return bool.from_bytes(_b, byteorder='big')
        elif k in self.integers:
            return int.from_bytes(_b, byteorder='big', signed=True)

    def bool_encoder(self, v):
        return bool.to_bytes(v, length=1, byteorder='big')

    def int_encoder(self, v):
        return int.to_bytes(v, length=4, byteorder='big', signed=True)

    def log_metadata(self, key, v_old):
        with open(self.log_file, "ab") as log_file:
            if key in self.booleans:
                write_line = WriteMetadataLine(key, v_old, self.bool_encoder)
            else:
                write_line = WriteMetadataLine(key, v_old, self.int_encoder)
            log_file.write(write_line.encode())

    def commit(self, chunk_id, worker_id=None):
        with open(self.log_file, "ab") as log_file:
            commit_line = CommitLine(chunk_id, worker_id)
            log_file.write(commit_line.encode())
