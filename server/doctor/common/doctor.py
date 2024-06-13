import logging
import signal
import time

from common.leaderManager import LeaderManager
from common.heartbeat import HeartBeat

from multiprocessing import Semaphore, Event

class Doctor:
    def __init__(self, config_params):
        signal.signal(signal.SIGTERM, self.__handle_signal)
        self.config_params = config_params
        self.on = True

        self.heartbeat_port = config_params["heartbeat_port"]
        self.leader_port = config_params["leader_port"]
        self.peers = config_params["peers"]
        self.id = config_params["peer_id"]

        self.my_ip = "doctor"+str(self.id)

        self.leader_token = Event()

        
    def run(self):
        self.heartbeat = HeartBeat(self.my_ip,
                                   self.heartbeat_port)
        
        self.leaderManager = LeaderManager(self.my_ip,
                                           self.leader_port,
                                           self.peers,
                                           self.id,
                                           self.leader_token)

        self.heartbeat.start()
        self.leaderManager.start()
        logging.info('action: run doctor | result: success')


        self.doctorloop()
        self.heartbeat.join()

    def doctorloop(self):
        while self.on:
            time.sleep(5)
            self.leader_token.wait()
            print("Soy el doctor activo")

                
    def __handle_signal(self, signum, frame):
        logging.info(f'action: stop_doctor | result: in_progress | signal: SIGTERM({signum})')
        self.on = False
        self.heartbeat.terminate()
        self.leaderManager.terminate()
        logging.info('action: stop_doctor | result: sucess')