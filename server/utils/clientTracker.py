from utils.model.log import LogFactory, LogLineType
from utils.persistentList import PersistentList
from utils.persistentMap import PersistentMap
from utils.logManager import LogManager

import os

BASE_DIRECTORY = '/clients'

EXPECTED = "EXPECT"
WORKED = "WORKED"
SENT = "SENT"


class ClientTracker():
    def __init__(self, client_id):

        if not os.path.exists(BASE_DIRECTORY):
            os.mkdir(BASE_DIRECTORY)

        if not os.path.exists(BASE_DIRECTORY + '/' + str(client_id)):
            os.mkdir(BASE_DIRECTORY + '/' + str(client_id))

        self.client_id = client_id
        self.log_manager = LogManager(client_id)
  
        self.worked_chunks = PersistentList(BASE_DIRECTORY + '/' + str(client_id) + '/chunks')
        self.meta_data = PersistentMap(BASE_DIRECTORY + '/' + str(client_id) + '/meta')
        self.data = PersistentMap(BASE_DIRECTORY + '/' + str(client_id) + '/data')
        
        self.meta_data[EXPECTED] = -1
        self.meta_data[WORKED] = 0
        self.meta_data[SENT] = 0

        self.results = {}
        

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
                #self.worked_chunks.append(chunk_id)
                #self.log_manager.append_chunk_id(chunk_id)
            return

        log_lines.reverse()
        for log_line in log_lines:

            if log_line.type == LogLineType.WRITE:
                self.data[log_line.key] = self.parser(log_line.key, log_line.old_value)

            elif log_line.type == LogLineType.WRITE_METADATA:
                self.meta_data[log_line.key] = log_line.old_value

            elif log_line.type == LogLineType.BEGIN:
                break
        self.flush_data()

    def recovery(self):
        self.meta_data.load(lambda k, v: v)
        self.worked_chunks.load()
        self.data.load(self.parser)

        if os.path.getsize(self.log_manager.log_file) > 0:
            self.undo()

    def expect(self, expected):
        self.meta_data[EXPECTED] = expected
    
    def add_worked(self, amount):
        self.meta_data[WORKED] += amount

    def add_sent(self, amount):
        self.meta_data[SENT] += amount

    def total_sent(self):
        return self.meta_data[SENT]    
    
    def is_completed(self):
        return self.meta_data[EXPECTED] == self.meta_data[WORKED]

    def flush_data(self):
        self.data.flush()
        self.meta_data.flush()

    def persist(self, chunk_id, size):
        self.log_manager.begin(chunk_id)
        self.log_manager.log_metadata(EXPECTED, self.meta_data[EXPECTED])
        self.log_manager.log_metadata(WORKED, self.meta_data[WORKED])
        self.log_manager.log_metadata(SENT, self.meta_data[SENT])
        self.log_manager.log_changes()
        self.add_worked(size)
        self.flush_data()
        self.worked_chunks.append(chunk_id) # append & flush chunk_id
        self.log_manager.commit(chunk_id)

    def __repr__(self) -> str:
        n_data = len(self.data)
        n_results = len(self.results)
        return f'ClientTracker(C_ID: {str(self.client_id)} | #data: {n_data} | #results: {n_results})'

    def __str__(self) -> str:
        return self.__repr__()