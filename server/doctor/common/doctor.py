import threading
import logging
import signal
import docker
import time

from common.messageHandler import MessageHandler, MessageType
from common.leaderManager import LeaderManager
from multiprocessing import Event

DEAD_THRESHOLD = 20
WAIT_TIME = 5

class Doctor:
    def __init__(self, config_params):
        signal.signal(signal.SIGTERM, self.__handle_signal)
        self.config_params = config_params
        self.on = True

        # Leader Election
        self.id = config_params["peer_id"]
        self.my_ip = "doctor"+str(self.id)
        self.leader_port = config_params["leader_port"]
        self.peers = config_params["peers"]
        self.leader_token = Event()

        # HealthChecker
        self.heartbeat_port = config_params["heartbeat_port"]
        self.nodes = {node: time.time() for node in config_params["nodes"]}
        self.UDPHandler = MessageHandler(self.my_ip, self.heartbeat_port)
        self.timer = threading.Event()

        # Containers
        clientDocker = docker.DockerClient(base_url='unix://var/run/docker.sock')
        containers = clientDocker.containers.list(all=True)
        self.containers = {container.name: container for container in containers}

        
    def run(self):
        self.leaderManager = LeaderManager(self.my_ip,
                                           self.leader_port,
                                           self.peers,
                                           self.id,
                                           self.leader_token)

        receive_message = threading.Thread(target=self.receive_message)

        self.leaderManager.start()
        receive_message.start()
        logging.info('action: run doctor | result: success')

        self.doctorloop()
        self.leaderManager.join()
        logging.info('action: run doctor | result: finish')
    
    def doctorloop(self):
        port = self.heartbeat_port
        while True:
            self.timer.wait(WAIT_TIME)
            self.leader_token.wait()
            if not self.on: break
           
            # Send HEALTHCHECK to nodes
            for ip in self.nodes.keys():
                self.UDPHandler.send_message((ip,port), MessageType.HEALTHCHECK, ip)
            
            # Check response from nodes
            for ip,t in self.nodes.items():
                if is_dead(t):
                    logging.info(f"{ip} is dead")
                    dead_container = self.containers[ip]
                    dead_container.start()
                else:
                    logging.info(f"{ip} is alive | last beat: {round(time.time() - t,3)}")
    
    def receive_message(self):
        while self.on:
            message, addr = self.UDPHandler.receive_message()
            if addr: self.handle_message(message, addr)

        self.UDPHandler.close()

    def handle_message(self, message, addr):
        mType = message["type"]
        id = message["id"]
        
        if mType == MessageType.HEALTHCHECK:
            self.UDPHandler.send_message(addr, MessageType.HEARTBEAT, id)
        if mType == MessageType.HEARTBEAT:
            self.nodes[id] = time.time() 
            
    def __handle_signal(self, signum, frame):
        logging.info(f'action: stop_doctor | result: in_progress | signal: SIGTERM({signum})')
        self.on = False
        self.UDPHandler.close()
        self.leaderManager.terminate()

        logging.info('action: stop_doctor | result: sucess')

def is_dead(t):
    return DEAD_THRESHOLD < time.time() - t