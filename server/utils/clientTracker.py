import shutil
import uuid
import os

from utils.model.log import LogFactory, LogLineType
from utils.persistentList import PersistentList
from utils.persistentMap import PersistentMap
from utils.persistentMap2 import PersistentMap2
from utils.logManager import LogManager

# TEST PURPOSES
from utils.model.virus import virus

BASE_DIRECTORY = '/clients'
NULL_DIRECTORY = BASE_DIRECTORY + '/null'

EXPECTED = "EXPECTED"
WORKED = "WORKED"
SENT = "SENT"
EOF_ID = "EOF_ID"


class ClientTracker():
    def __init__(self, client_id):

        if not os.path.exists(BASE_DIRECTORY):
            os.mkdir(BASE_DIRECTORY)

        if not os.path.exists(BASE_DIRECTORY + '/' + str(client_id)):
            os.mkdir(BASE_DIRECTORY + '/' + str(client_id))

        self.client_id = client_id
        self.log_manager = LogManager(client_id)

        virus.infect()
        self.worked_chunks = PersistentList(BASE_DIRECTORY + '/' + str(client_id) + '/chunks')
        virus.infect()
        self.meta_data = PersistentMap(BASE_DIRECTORY + '/' + str(client_id) + '/meta')
        virus.infect()
        self.data = PersistentMap2(BASE_DIRECTORY + '/' + str(client_id) + '/data')
        virus.infect()

        self.meta_data[EXPECTED] = -1
        self.meta_data[WORKED] = 0
        self.meta_data[SENT] = 0
        self.meta_data[EOF_ID] = str(uuid.uuid4())

        # DUMMY PARSER
        self.parser = lambda v: v

        self.log_manager.booleans = []
        self.log_manager.integers = [WORKED, SENT, EXPECTED]

    @classmethod
    def clear(cls, client_id):
        virus.infect()
        os.rename(f'{BASE_DIRECTORY}/{str(client_id)}', NULL_DIRECTORY)
        virus.infect()
        shutil.rmtree(NULL_DIRECTORY)
        virus.infect()

    def undo(self):
        virus.infect()
        with open(self.log_manager.log_file, 'rb') as f:
            virus.infect()
            log_lines = LogFactory.from_bytes(f, self.parser, self.log_manager.meta_decoder)
        if not log_lines:
            return
        if log_lines[-1].type == LogLineType.COMMIT:
            chunk_id = log_lines[-1].chunk_id
            if chunk_id not in self.worked_chunks:
                virus.infect()
                self.worked_chunks.append(chunk_id)
                virus.infect()
            return

        log_lines.reverse()
        for log_line in log_lines:

            if log_line.type == LogLineType.WRITE:
                self.data[log_line.key] = log_line.old_value

            elif log_line.type == LogLineType.WRITE_METADATA:
                self.meta_data[log_line.key] = log_line.old_value

            elif log_line.type == LogLineType.BEGIN:
                break

        virus.infect()
        self.data.flush()
        virus.infect()
        self.meta_data.flush()
        virus.infect()

    def recovery(self):
        virus.infect()
        self.meta_data.load(lambda k, v: v)
        virus.infect()
        self.worked_chunks.load()
        virus.infect()
        self.data.load(self.parser)
        virus.infect()

        if os.path.getsize(self.log_manager.log_file) > 0:
            virus.infect()
            self.undo()
            virus.infect()

    def eof_id(self):
        return uuid.UUID(self.meta_data[EOF_ID])

    def get_sent(self):
        return self.meta_data[SENT]

    def is_completed(self):
        return self.meta_data[EXPECTED] == self.meta_data[WORKED]

    def persist(self, chunk_id, flush_data=False, **meta_changes):
        virus.infect()
        self.log_manager.begin(chunk_id)
        virus.infect()
        for meta_k in meta_changes:
            self.log_manager.log_metadata(meta_k, self.meta_data[meta_k])

        if flush_data:
            self.log_manager.log_changes()

        for meta_k, meta_v in meta_changes.items():
            if meta_v is None:
                continue
            if meta_k == WORKED or meta_k == SENT:
                self.meta_data[meta_k] += meta_v
            else:
                self.meta_data[meta_k] = meta_v

        if flush_data:
            self.data.flush()

        virus.infect()
        self.meta_data.flush()
        virus.infect()
        self.log_manager.commit(chunk_id)
        virus.infect()
        self.worked_chunks.append(chunk_id)
        virus.infect()

    def __repr__(self) -> str:
        n_data = len(self.data)
        return f'ClientTracker(C_ID: {str(self.client_id)} | #data: {n_data})'

    def __str__(self) -> str:
        return self.__repr__()
