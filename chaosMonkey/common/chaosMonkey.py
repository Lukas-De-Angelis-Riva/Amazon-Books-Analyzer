import logging
import random
import signal
import docker
import time

from multiprocessing import Event

class ChaosMonkey():

    def __init__(self, config_params):
        signal.signal(signal.SIGTERM, self.__handle_signal)
        self.config_params = config_params
        self.finish = Event()
        self.waiting_time = config_params["waiting_time"]
        clientDocker = docker.DockerClient(base_url='unix://var/run/docker.sock')
        containers = clientDocker.containers.list(all=True)
        self.containers = {container.name: container for container in containers if (container.name in config_params["nodes"])}



    def run(self):
        while True:
            self.finish.wait(self.waiting_time)

            if self.finish.is_set():
                break
            
            keys = list(self.containers.keys())
            victim = random.choice(keys)

            container = self.containers[victim]
            
            if victim not in ["clientHandler","resultHandler"]:
                try:
                    logging.info(f"Try to kill: {victim}:{container}")
                    container.kill()
                except docker.errors.APIError as e:
                    logging.info(f'action: dockerAPIError | {e}')


    def __handle_signal(self, signum, frame):
        logging.info(f'action: stop_chaos_monkey | result: in_progress | signal: SIGTERM({signum})')
        self.finish.set()
        logging.info('action: stop_chaos_monkey | result: sucess')