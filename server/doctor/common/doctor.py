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
   procesos con números más altos. []

5. Si P recibe un mensaje del Coordinador, trata al remitente como el coordinador.[X]

https://github.com/alperari/bully/blob/main/bully.py ????
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

        self.messageHandler = MessageHandler(self.my_ip, self.port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.my_ip , self.port))

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


    def send_message(self, message, ip):
        try:
            self.sock.sendto(message.encode(), (ip, self.port))
        except socket.gaierror as e:
            print(f"{ip} is dead")

    def receive_message(self):
        while self.on:
            data, addr = self.sock.recvfrom(1024)
            message = data.decode()
            self.handle_message(message, addr)



    def handle_message(self, message, sender_ip):
        if message.startswith("ELECTION"):
            sender_id = int(message.split()[1])
            if sender_id < self.process_id:
                self.send_message(f"OK {self.process_id}", sender_ip[0])
                if not self.get_in_election(): 
                    self.start_election()

        elif message.startswith("OK"):
            self.set_ok_received(True)
            print(f"Received OK from {sender_ip}")

        elif message.startswith("COORDINATOR"):
            sender_id = int(message.split()[1])

            if sender_id < self.process_id:
                if not self.get_in_election(): 
                    self.start_election()
            else:
                self.set_leader_id(int(message.split()[1]))
                self.set_in_election(False)
                print(f"[COORDINATOR] {self.leader_id} is the leader")


    

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
                self.send_message(f"ELECTION {self.process_id}", peer_ip)
    
        # Wait a bit for responses
        threading.Timer(5,self.result_election).start()

    def result_election(self):
        if not self.ok_received and not self.leader_id:
            print(f"[ELECTION] start Coordinator soy leader")
            return self.announce_coordinator()

        if self.ok_received and not self.leader_id:
            print(f"[ELECTION] start election soy leader")
            return self.start_election()

    def announce_coordinator(self):
        print(f"[COORDINATOR] {self.my_ip}:{self.port} is the leader")
        self.leader_id = self.process_id
        for peer in self.peers:
            if not self.is_my_ip(peer):
                self.send_message(f"COORDINATOR {self.process_id}", peer)

    def run(self):
        if self.process_id == self.peers_amount:
            return
        
        threading.Thread(target=self.receive_message).start()
        time.sleep(5)  # Wait for other processes to start

        self.start_election()

        while self.on:
            time.sleep(3)
            print(f"{self.leader_id} is the leader")
            self.func()

    def func(self):
        data = self.messageHandler.serialize(MessageType.OK,self.process_id,)
        print(data)

        msg = self.messageHandler.deserialize(data)
        print(msg)

    def __handle_signal(self, signum, frame):
        logging.info(f'action: stop_server | result: in_progress | signal {signum}')
        self.on = False
        self.sock.close()
        logging.debug('action: shutdown_socket | result: success')
