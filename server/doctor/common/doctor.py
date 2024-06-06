import threading
import logging
import signal
import socket
import time

from common.messageHandler import MessageHandler 
from common.messageHandler import MessageType
"""
Algoritmo de Bully:

1. Si P tiene el ID de proceso más alto, envía un mensaje de Victoria a todos 
   los demás procesos y se convierte en el nuevo Coordinador. De lo contrario, 
   P transmite un mensaje de elección a todos los demás procesos con ID de 
   proceso superiores a él. [X]

2. Si P no recibe respuesta después de enviar un mensaje de elección, transmite
   un mensaje de victoria a todos los demás procesos y se convierte en el 
   coordinador. [X]

3. Si P recibe una Respuesta de un proceso con un ID superior, no envía más 
   mensajes para esta elección y espera un mensaje de Victoria. (Si no hay ningún
   mensaje de Victoria después de un período de tiempo, se reinicia el proceso 
   desde el principio). [X]

4. Si P recibe un mensaje de elección de otro proceso con un ID inferior, envía
   un mensaje de respuesta y, si aún no ha iniciado una elección, inicia el 
   proceso de elección desde el principio, enviando un mensaje de elección a los
   procesos con números más altos. [X]

5. Si P recibe un mensaje del Coordinador, trata al remitente como el coordinador.[X]
"""

class Doctor:
    def __init__(self, config_params):
        self.on = True
        self.port = config_params["port"]
        self.peers_amount = config_params["peers"]
        self.process_id = config_params["peer_id"]
        self.my_ip = "doctor"+str(config_params["peer_id"])
        self.peers = ["doctor"+str(i) for i in range(1,self.peers_amount+1)]

        self.leader_id = None
        self.ok_received = False
        self.in_election= False
        self.lock_leader_id = threading.Lock()
        self.lock_ok_received= threading.Lock()
        self.lock_in_election = threading.Lock()

        self.patients = {}
        self.last_heartbeat = 0
        self.event = threading.Event()

        self.messageHandler = MessageHandler(self.my_ip, self.port)

        signal.signal(signal.SIGTERM, self.__handle_signal)

    def set_leader_id(self, leader_id):
        with self.lock_leader_id:
            self.leader_id = leader_id
    
    def get_leader_id(self):
        with self.lock_leader_id:
            return self.leader_id

    def set_ok_received(self, ok_received):
        with self.lock_ok_received:
            self.ok_received = ok_received
    
    def get_ok_received(self):
        with self.lock_ok_received:
            return self.ok_received

    def set_in_election(self, in_election):
        with self.lock_in_election:
            self.in_election = in_election
    
    def get_in_election(self):
        with self.lock_in_election:
            return self.in_election
        
    def is_my_ip(self, ip):
        return self.my_ip == ip
    
    def higher_ip(self, ip):
        return self.my_ip < ip
    
    def am_leader(self):
        return self.get_leader_id() == self.process_id

    def receive_message(self):
        while self.on:
            message, addr = self.messageHandler.receive_message()
            self.handle_message(message, addr)

    def handle_message(self, message, addr):
        mType = message["type"]
        
        if mType == MessageType.ELECTION:
            self.handle_election_message(message,addr)
        if mType == MessageType.OK:
            self.handler_ok_message(addr)
        if mType == MessageType.COORDINATOR:
            self.handler_coordinator_message(message)
        if mType == MessageType.HEARTBEAT:
            self.handle_heartbeat_message(message, addr)

    def handle_election_message(self, message, addr):
        sender_id = message["id"]
        if sender_id < self.process_id:
            self.messageHandler.send_message(addr, MessageType.OK)
            if not self.get_in_election(): 
                self.start_election()

    def handler_ok_message(self,addr):
        self.set_ok_received(True)
        print(f"[OK] Received OK from {addr}")

    def handler_coordinator_message(self, message):
        sender_id = message["id"]
        if sender_id < self.process_id:
            if not self.get_in_election(): 
                self.start_election()
        else:
            self.set_leader_id(sender_id)
            self.set_in_election(False)
            print(f"[COORDINATOR] {self.leader_id} is the leader")

    def handle_heartbeat_message(self,message, addr):
        sender_id = message["id"]
        if self.am_leader():
            self.patients[sender_id] = True
        else:
            self.last_heartbeat = time.time()
            self.messageHandler.send_message(addr, MessageType.HEARTBEAT,self.process_id)

    def start_election(self):
        self.set_in_election(True)
        if self.process_id == self.peers_amount:
            return self.announce_coordinator()
             
        self.set_ok_received(False)
        for peer_ip in self.peers:
            if not self.is_my_ip(peer_ip) and \
                self.higher_ip(peer_ip) and \
                self.get_ok_received() == False:
                print(f"[ELECTION] send msg to {peer_ip}:{self.port}")
                self.messageHandler.send_message((peer_ip,self.port),
                                                 MessageType.ELECTION,
                                                 self.process_id)
                
        # Wait a bit for responses
        threading.Timer(5,self.result_election).start()

    def result_election(self):
        if not self.get_ok_received() and not self.get_leader_id():
            print(f"[ELECTION] start Coordinator soy leader")
            return self.announce_coordinator()

        if self.get_ok_received() and not self.get_leader_id():
            print(f"[ELECTION] start election soy leader")
            return self.start_election()

    def announce_coordinator(self):
        print(f"[COORDINATOR] {self.my_ip}:{self.port} is the leader")
        self.set_leader_id(self.process_id)
        for peer_ip in self.peers:
            if not self.is_my_ip(peer_ip):
                self.messageHandler.send_message((peer_ip,self.port),
                                                 MessageType.COORDINATOR,
                                                 self.process_id)


    def run(self):
        threading.Thread(target=self.receive_message).start()
        time.sleep(5)  # Wait for other processes to start

        self.start_election()
        self.liveloop()

    def is_leader_alive(self):
        return 15>(time.time() - self.last_heartbeat)  

    def liveloop(self):
        self.last_heartbeat = time.time()
        while self.on:
            if self.am_leader():   
                print(f"{self.process_id} is the leader")
                self.heald_check()
            else:
                self.event.wait(5)  
                if self.is_leader_alive():
                    print(f"{self.get_leader_id()} is the leader")
                else:
                    print("El lider a muerto")
                    self.set_leader_id(None)
                    self.start_election()

    def heald_check(self):
        for peer_ip in self.peers:
            self.messageHandler.send_message((peer_ip,self.port),
                                             MessageType.HEARTBEAT,
                                             self.process_id)
            
        self.event.wait(2)


    def __handle_signal(self, signum, frame):
        logging.info(f'action: stop_server | result: in_progress | signal {signum}')
        self.on = False
        self.messageHandler.close()
        logging.debug('action: shutdown_socket | result: success')


"""
ToDo: todos levantados menos el 4 y lo levanto

"""