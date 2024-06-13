import pickle
import socket

MAXSIZE = 1024

class MessageType():
    ELECTION = 0
    OK = 1
    COORDINATOR = 2
    HEARTBEAT = 3

class MessageHandler:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((ip , port))

    def serialize(self, type, id=None):
        message = {"type":type}
        if id : message["id"] = id

        return pickle.dumps(message)   
    
    def deserialize(self, data):
        return pickle.loads(data)

    def send_message(self, addr, type, id=None):
        try:
            message = self.serialize(type,id)
            self.sock.sendto(message, addr)
        except socket.gaierror as e:
            return 0
        finally:
            return len(message)
        
    def receive_message(self):
        try:
            data, addr = self.sock.recvfrom(MAXSIZE)
            message = self.deserialize(data)
        except socket.error as e:
            message = {}
            addr = None
        finally:
            return message, addr
    
    def close(self):
        self.sock.close()