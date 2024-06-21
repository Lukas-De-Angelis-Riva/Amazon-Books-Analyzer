from utils.model.log import WriteLine, WriteMetadataLine, BeginLine, CommitLine

import csv
import os

class LogManager():
    BASE_DIRECTORY = '/clients'

    def __init__(self, client_id):
        self.client_id = client_id

        self.log_file = LogManager.BASE_DIRECTORY + '/' + str(client_id) + '/log'
        if not os.path.exists(self.log_file):
            open(self.log_file, 'w').close()

        self.changes = {}

    def begin(self, chunk_id):
        with open(self.log_file, "w", encoding='UTF-8') as log_file:
            begin_line = BeginLine(chunk_id)
            log_file.write(begin_line.to_line())

    def hold_change(self, k, v_old, v_new):
        if k not in self.changes:
            self.changes[k] = [v_old, v_new]
        else:
            self.changes[k][1] = v_new

    def log_changes(self):
        with open(self.log_file, "a+") as log_file:
            for k in self.changes:
                old = self.changes[k][0].encode()
                write_line = WriteLine(k, old)
                log_file.write(write_line.to_line())

        self.changes = {}

    def log_metadata(self, key, v_old):
        with open(self.log_file, "a+") as log_file:
            write_line = WriteMetadataLine(key, v_old)
            log_file.write(write_line.to_line())

    def commit(self, chunk_id):
        with open(self.log_file, "a+") as log_file:
            commit_line = CommitLine(chunk_id)
            log_file.write(commit_line.to_line())
