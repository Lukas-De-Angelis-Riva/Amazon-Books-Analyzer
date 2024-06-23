import shutil
import uuid
import os

from utils.model.log import LogFactory, LogLineType
from utils.persistentList import PersistentList
from utils.persistentMap import PersistentMap
from utils.logManager import LogManager

# TEST PURPOSES
from utils.model.virus import virus

BASE_DIRECTORY = '/clients'
NULL_DIRECTORY = BASE_DIRECTORY + '/null'

EXPECTED = "EXPECT"
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
        self.data = PersistentMap(BASE_DIRECTORY + '/' + str(client_id) + '/data')
        virus.infect()

        self.meta_data[EXPECTED] = -1
        self.meta_data[WORKED] = 0
        self.meta_data[SENT] = 0
        self.meta_data[EOF_ID] = str(uuid.uuid4())

        # DUMMY PARSER
        self.parser = lambda k, v: v

    def undo(self):
        virus.infect()
        with open(self.log_manager.log_file, 'r') as f:
            virus.infect()
            aux = f.readlines()
        virus.infect()
        log_lines = LogFactory.from_lines(aux)
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
                self.data[log_line.key] = self.parser(log_line.key, log_line.old_value)

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

    def clear(self):
        virus.infect()
        os.rename(f'{BASE_DIRECTORY}/{str(self.client_id)}', NULL_DIRECTORY)
        virus.infect()
        shutil.rmtree(NULL_DIRECTORY)
        virus.infect()

    def eof_id(self):
        return uuid.UUID(self.meta_data[EOF_ID])

    def get_sent(self):
        return self.meta_data[SENT]

    def is_completed(self):
        return self.meta_data[EXPECTED] == self.meta_data[WORKED]

    def persist(self, chunk_id, worked=None, expected=None, sent=None):
        virus.infect()
        self.log_manager.begin(chunk_id)
        virus.infect()
        if expected is not None:
            self.log_manager.log_metadata(EXPECTED, self.meta_data[EXPECTED])
        virus.infect()
        if sent is not None:
            self.log_manager.log_metadata(SENT, self.meta_data[SENT])
        virus.infect()
        if worked is not None:
            self.log_manager.log_metadata(WORKED, self.meta_data[WORKED])
            self.log_manager.log_changes()
        virus.infect()

        if expected is not None:
            self.meta_data[EXPECTED] = expected
        if sent is not None:
            self.meta_data[SENT] += sent
        if worked is not None:
            self.meta_data[WORKED] += worked
            virus.infect()
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
