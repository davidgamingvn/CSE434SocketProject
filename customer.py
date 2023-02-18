import socket
import math


class Customer:

    ## assigned port ranges
    GROUP_NUMBER = 39
    PORT_START = math.ceil(GROUP_NUMBER / 2) * 1000 + 500
    PORT_END = math.ceil(GROUP_NUMBER / 2) * 1000 + 999

    BUFFER_SIZE = 1024
    HOST = socket.gethostname()
    IP = socket.gethostbyname(HOST)

    PORT = PORT_START
    SERVER_ADDR = ("34.125.218.27", PORT)   # the server's address

    def __init__(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # IP/UDP
        self.sock = sock

    def send(self, msg):
        message = str.encode(msg)

        self.sock.sendto(message, Customer.SERVER_ADDR)     # connect to the server, send a command

        recv = self.sock.recvfrom(Customer.BUFFER_SIZE)     # receive any response sent from server
        print(recv)


if __name__ == "__main__":
    customer = Customer()

    while True:
        command = input("msg: ")
        customer.send(command)
