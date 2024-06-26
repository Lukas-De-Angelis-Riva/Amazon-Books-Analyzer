import shutil
import uuid
import os
import io

from utils.model.log import LogFactory, LogLineType
from utils.persistentList2 import PersistentList2
from utils.persistentMap import PersistentMap
from utils.persistentMap2 import PersistentMap2
from utils.logManager import LogManager

# TEST PURPOSES
from utils.model.virus import virus


BASE_DIRECTORY = "/clients"
NULL_DIRECTORY = BASE_DIRECTORY + '/null'
WORKED_BY_WORKER = "WORKED"
TOTAL_BY_WORKER = "TOTAL"
EOF_ID = "EOF_ID"


class ClientTrackerSynchronizer():
    def __init__(self, client_id, n_workers):

        if not os.path.exists(BASE_DIRECTORY):
            os.mkdir(BASE_DIRECTORY)

        if not os.path.exists(BASE_DIRECTORY + '/' + str(client_id)):
            os.mkdir(BASE_DIRECTORY + '/' + str(client_id))

        self.client_id = client_id
        self.n_workers = n_workers
        self.log_manager = LogManager(client_id)

        self.worked_chunks = PersistentList2(BASE_DIRECTORY + '/' + str(client_id) + '/' + 'chunks')
        self.meta_data = PersistentMap(BASE_DIRECTORY + '/' + str(client_id) + '/' + "meta")
        self.data = PersistentMap2(BASE_DIRECTORY + '/' + str(client_id) + '/' + "data")

        self.meta_data[WORKED_BY_WORKER] = {str(i): 0 for i in range(1, n_workers+1)}
        self.meta_data[TOTAL_BY_WORKER] = {str(i): -1 for i in range(1, n_workers+1)}
        self.meta_data[EOF_ID] = str(uuid.uuid4())

        # DUMMY PARSER
        self.parser = lambda v: v
        self.log_manager.booleans = []
        self.log_manager.integers = [WORKED_BY_WORKER, TOTAL_BY_WORKER]

    def undo(self):
        with open(self.log_manager.log_file, 'rb') as f:
            aux = f.read()
        log_lines = LogFactory.from_bytes(io.BytesIO(aux), self.parser, self.log_manager.meta_decoder)
        if not log_lines:
            return
        if log_lines[-1].type == LogLineType.COMMIT:
            chunk_id = log_lines[-1].chunk_id
            worker_id = log_lines[-1].worker_id
            if chunk_id not in self.worked_chunks:
                virus.infect()
                self.worked_chunks.append((chunk_id,int(worker_id)))
                virus.infect()
            return

        worker_id = str(log_lines[0].worker_id)
        log_lines.reverse()
        for log_line in log_lines:
            if log_line.type == LogLineType.WRITE_METADATA:
                self.meta_data[log_line.key][worker_id] = log_line.old_value
            elif log_line.type == LogLineType.BEGIN:
                break

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
            self.undo()
            virus.infect()

    def all_chunks_received(self):
        return all(
            (self.meta_data[TOTAL_BY_WORKER][str(i)] == self.meta_data[WORKED_BY_WORKER][str(i)])
            for i in range(1, self.n_workers+1)
        )

    def total_worked(self):
        return sum(self.meta_data[TOTAL_BY_WORKER].values())

    def eof_id(self):
        return uuid.UUID(self.meta_data[EOF_ID])

    def clear(self):
        virus.infect()
        os.rename(f'{BASE_DIRECTORY}/{str(self.client_id)}', NULL_DIRECTORY)
        virus.infect()
        shutil.rmtree(NULL_DIRECTORY)
        virus.infect()

    def persist(self, chunk_id, worker_id, worked=None, total=None):
        virus.infect()
        self.log_manager.begin(chunk_id, int(worker_id))
        virus.infect()
        if worked is not None:
            self.log_manager.log_metadata(WORKED_BY_WORKER, self.meta_data[WORKED_BY_WORKER][worker_id])
        if total is not None:
            self.log_manager.log_metadata(TOTAL_BY_WORKER, self.meta_data[TOTAL_BY_WORKER][worker_id])
        if total is not None:
            self.meta_data[TOTAL_BY_WORKER][worker_id] = total
        if worked is not None:
            self.meta_data[WORKED_BY_WORKER][worker_id] += worked
            self.data.flush()
        virus.infect()
        self.meta_data.flush()
        virus.infect()
        self.log_manager.commit(chunk_id, int(worker_id))
        virus.infect()
        # append & flush chunk_id
        self.worked_chunks.append((chunk_id,int(worker_id)))
        virus.infect()

    def __repr__(self) -> str:
        return f'ClientTracker({self.client_id})'

    def __str__(self) -> str:
        return self.__repr__()
