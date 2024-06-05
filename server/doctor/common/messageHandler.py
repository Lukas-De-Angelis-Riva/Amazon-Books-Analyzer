import pickle
import socket

MAXSIZE = 1024

class MessageType():
    ELECTION = 0
    OK = 1
    COORDINATOR = 2


class MessageHandler:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        # self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # self.sock.bind((ip , port))

    def serialize(self, type, id=None):
        message = {"type":type}
        if id : message["id"] = id

        return pickle.dumps(message)   
    
    def deserialize(self, data):
        return pickle.loads(data)

    def send_message(self, ip, type, id=None):
        message = self.serialize(type,id)
        self.sock.sendto(message.encode(), (ip, self.port))
   
    def receive_message(self):
        data, addr = self.sock.recvfrom(MAXSIZE)
        message = self.deserialize(data)
        return message, addr