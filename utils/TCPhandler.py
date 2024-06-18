class SocketBroken(Exception):
    pass


# This class will handle the TCP socket management,
# preventing "short reads" and "short sends."


class TCPHandler:

    def __init__(self, socket):
        self.socket = socket

    def read(self, bytes_to_read):
        """
        Receives and returns byte data from the client preventing short reads.
        """
        data = b''
        while len(data) < bytes_to_read:
            bytes = self.socket.recv(bytes_to_read - len(data))
            if bytes == b'':
                raise SocketBroken()
            data += bytes
        return data

    def send_all(self, msg):
        """
        Recv all n bytes to avoid short read
        """
        bytesSended = 0
        while bytesSended < len(msg):
            b = self.socket.send(msg[bytesSended:])
            if b == 0:
                raise SocketBroken()
            bytesSended += b
        return bytesSended

    def close(self):
        self.socket.close()
