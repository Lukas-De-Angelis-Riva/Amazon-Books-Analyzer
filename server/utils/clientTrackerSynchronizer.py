from utils.model.log import LogFactory, LogLineType
from utils.persistentList import PersistentList
from utils.persistentMap import PersistentMap
from utils.logManager import LogManager

import os


BASE_DIRECTORY = "/clients"
WORKED_BY_WORKER = "WORKED"
TOTAL_BY_WORKER = "TOTAL"


class ClientTrackerSynchronizer():
    def __init__(self, client_id, n_workers):

        if not os.path.exists(BASE_DIRECTORY):
            os.mkdir(BASE_DIRECTORY)

        if not os.path.exists(BASE_DIRECTORY + '/' + str(client_id)):
            os.mkdir(BASE_DIRECTORY + '/' + str(client_id))

        self.client_id = client_id
        self.n_workers = n_workers
        self.log_manager = LogManager(client_id)

        self.worked_chunks = PersistentList(BASE_DIRECTORY + '/' + str(client_id) + '/' + 'chunks')
        self.meta_data = PersistentMap(BASE_DIRECTORY + '/' + str(client_id) + '/' + "meta")
        self.data = PersistentMap(BASE_DIRECTORY + '/' + str(client_id) + '/' + "data")

        self.meta_data[WORKED_BY_WORKER] = {str(i): 0 for i in range(1, n_workers+1)}
        self.meta_data[TOTAL_BY_WORKER] = {str(i): -1 for i in range(1, n_workers+1)}

        # DUMMY PARSER
        self.parser = lambda k, v: v

    def undo(self):
        with open(self.log_manager.log_file, 'r') as f:
            aux = f.readlines()
        log_lines = LogFactory.from_lines(aux)
        if not log_lines:
            return
        if log_lines[-1].type == LogLineType.COMMIT:
            chunk_id = log_lines[-1].chunk_id
            if chunk_id not in self.worked_chunks:
                self.worked_chunks.append(chunk_id)
            return

        worker_id = log_lines[0].worker_id
        log_lines.reverse()
        for log_line in log_lines:

            if log_line.type == LogLineType.WRITE:
                self.data[log_line.key] = self.parser(log_line.key, log_line.old_value)

            elif log_line.type == LogLineType.WRITE_METADATA:
                self.meta_data[WORKED_BY_WORKER][worker_id] = log_line.old_value

            elif log_line.type == LogLineType.BEGIN:
                break
        self.flush_data()

    def recovery(self):
        self.meta_data.load(lambda k, v: v)
        self.worked_chunks.load()
        self.data.load(self.parser)

        if os.path.getsize(self.log_manager.log_file) > 0:
            self.undo()

    def all_chunks_received(self):
        return all(
            (self.meta_data[TOTAL_BY_WORKER][str(i)] == self.meta_data[WORKED_BY_WORKER][str(i)])
            for i in range(1, self.n_workers+1)
        )

    def total_worked(self):
        return sum(self.meta_data[TOTAL_BY_WORKER].values())

    def add_worked(self, amount, worker_id):
        self.meta_data[WORKED_BY_WORKER][worker_id] += amount

    def set_total(self, total, worker_id):
        self.meta_data[TOTAL_BY_WORKER][worker_id] = total

    def flush_data(self):
        self.data.flush()
        self.meta_data.flush()

    def persist(self, chunk_id, worker_id, size):
        self.log_manager.begin(chunk_id, worker_id)
        self.log_manager.log_metadata(WORKED_BY_WORKER, self.meta_data[WORKED_BY_WORKER][worker_id])
        self.log_manager.log_metadata(TOTAL_BY_WORKER, self.meta_data[TOTAL_BY_WORKER][worker_id])
        self.log_manager.log_changes()
        self.add_worked(size, worker_id)
        self.flush_data()
        self.log_manager.commit(chunk_id, worker_id)
        # append & flush chunk_id
        self.worked_chunks.append(chunk_id)

    def __repr__(self) -> str:
        return f'ClientTracker({self.client_id})'

    def __str__(self) -> str:
        return self.__repr__()
