import socket
import math


class Customer:
    GROUP_NUMBER = 39
    PORT_START = math.ceil(GROUP_NUMBER / 2) * 1000 + 500
    PORT_END = math.ceil(GROUP_NUMBER / 2) * 1000 + 999

    BUFFER_SIZE = 1024
    HOST = socket.gethostname()
    IP = socket.gethostbyname(HOST)

    PORT = PORT_START
    SERVER_ADDR = ("127.0.0.1", PORT)

    def __init__(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # IP/UDP
        self.sock = sock

    def send(self, msg):
        message = str.encode(msg)

        self.sock.sendto(message, Customer.SERVER_ADDR)

        recv = self.sock.recvfrom(Customer.BUFFER_SIZE)
        print(recv)


if __name__ == "__main__":
    customer = Customer()

    while True:
        command = input("msg: ")
        customer.send(command)
