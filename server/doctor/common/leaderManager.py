from common.messageHandler import MessageHandler, MessageType
from multiprocessing import Process

import threading
import logging
import signal
import time

class LeaderManager(Process):
    def __init__(self, ip, port, peers_amount, id, doctor_token):
        super().__init__(name='LeaderManager', args=())
        self.on = True
        self.peers_amount = peers_amount
        self.id = id
        self.my_ip = "doctor"+str(id)
        self.port = port
        self.peers = ["doctor"+str(i) for i in range(1,peers_amount+1)][::-1]
        self.messageHandler = MessageHandler(ip, port)
        
        self.doctor_token = doctor_token

        self.leader_id = None
        self.lock_leader_id = threading.Lock()
   
        self.ok_received = threading.Event()
        self.leader_elected = threading.Event()     

        self.last_heartbeat = 0
        self.event = threading.Event()

    def is_my_ip(self, ip):
        return self.my_ip == ip
    
    def higher_ip(self, ip):
        return self.my_ip < ip
    
    def am_leader(self):
        return self.leader_id == self.id

    def receive_message(self):
        while self.on:
            message, addr = self.messageHandler.receive_message()
            if addr != None:
                self.handle_message(message, addr)

    def handle_message(self, message, addr):
        mType = message["type"]

        if mType == MessageType.ELECTION:
            self.handle_election_message(message,addr)
        if mType == MessageType.OK:
            self.handler_ok_message(message)
        if mType == MessageType.COORDINATOR:
            self.handler_coordinator_message(message)
        if mType == MessageType.HEARTBEAT:
            self.handle_heartbeat_message(message, addr)

    def handle_election_message(self, message, addr):
        sender_id = message["id"]
        if sender_id < self.id:
            self.messageHandler.send_message(addr, MessageType.OK, self.id)
            self.start_election()
            
    def handler_ok_message(self, message):
        self.ok_received.set()
        sender_id = message["id"]
        logging.info(f"action: recv_ok | id={sender_id} | result: success")

    def handler_coordinator_message(self, message):
        sender_id = message["id"]
        if sender_id < self.id:
            self.start_election()
        else:
            if self.id == sender_id: self.doctor_token.set()

            if self.am_leader() and self.id != sender_id:
                self.doctor_token.clear()

            self.leader_id = sender_id
            self.leader_elected.set()
            self.ok_received.set()
            logging.info(f"action: recv_coordinator | result: success")

    def handle_heartbeat_message(self,message, addr):
        self.last_heartbeat = time.time()

    def start_election(self):
        logging.info(f"action: election | result: in_progress")
        self.leader_id = None
        self.ok_received.clear()
        self.leader_elected.clear()

        if self.id == self.peers_amount:
            return self.announce_coordinator()
        
        for peer_ip in self.peers:
            if self.higher_ip(peer_ip) and \
                not self.is_my_ip(peer_ip) and \
                not self.ok_received.is_set() and \
                not self.leader_elected.is_set():
                
                self.messageHandler.send_message((peer_ip,self.port),
                                                MessageType.ELECTION,
                                                self.id)
                
        # Wait a bit for responses
        self.ok_received.wait(timeout=5)

        if not self.ok_received.is_set() and not self.leader_elected.is_set():
            logging.info(f"action: election | result announce_coordinator")            
            return self.announce_coordinator()
            

        # Wait a bit for responses
        self.leader_elected.wait(timeout=5)

        if self.ok_received.is_set() and not self.leader_elected.is_set():
            logging.info(f"action: election | result: restart") 
            return self.start_election()
        
        logging.info(f"action: election | result: success")
    

    def announce_coordinator(self):
        for peer_ip in self.peers:
            self.messageHandler.send_message((peer_ip,self.port),
                                                MessageType.COORDINATOR,
                                                self.id)

    def is_leader_alive(self):
        return 15 > (time.time() - self.last_heartbeat)  
    
    def check_leader(self):
        if not self.is_leader_alive() and self.leader_id:
            logging.info(f"action: leader_dead | result: in_progress")
            self.start_election()
            self.last_heartbeat = time.time()
            logging.info(f"action: leader_dead | result: success")

    def heald_check(self):
        for peer_ip in self.peers:
            self.messageHandler.send_message((peer_ip,self.port),
                                            MessageType.HEARTBEAT,
                                            self.id)

    def run(self):
        signal.signal(signal.SIGTERM, self.__handle_signal)
        receive_message = threading.Thread(target=self.receive_message)

        receive_message.start()

        time.sleep(5)

        self.start_election()
        self.last_heartbeat = time.time()

        while self.on :
            self.event.wait(2)
            self.leader_elected.wait()
            if self.am_leader():   
                #logging.info(f"action: heald_check  | leader: {self.leader_id}")
                self.heald_check()
            else:
                #logging.info(f"action: leader_check | leader: {self.leader_id}")
                self.check_leader()
        
        receive_message.join()

    def __handle_signal(self, signum, frame):
        logging.debug('action: stop_heartbeat | result: in_progress')
        self.on = False
        self.messageHandler.close()
        logging.debug('action: stop_heartbeat | result: success')

