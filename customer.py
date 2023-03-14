import socket
import math
import json
import threading

class Checkpoint:

    def __init__(self) -> None:
        self.willing_to_checkpoint = True
        self.last_recv_msg = {}
        self.first_send_msg = {}


class Customer:

    # assigned port ranges
    GROUP_NUMBER = 39
    PORT_START = math.ceil(GROUP_NUMBER / 2) * 1000 + 500
    PORT_END = math.ceil(GROUP_NUMBER / 2) * 1000 + 999

    BUFFER_SIZE = 1024

    PORT = PORT_START
    # SERVER_ADDR = ("34.125.218.27", PORT)   # the server's address
    SERVER_ADDR = ("127.0.0.1", PORT)

    def __init__(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # IP/UDP
        self.sock = sock

        self.initialized = False
        self.name = None
        self.balance = None
        self.cohort = None

    def send(self, addr, msg):
        message = str.encode(msg)

        # connect to the server, send a command
        self.sock.sendto(message, addr)

        # receive any response sent from server
        byte_message, addr = self.sock.recvfrom(Customer.BUFFER_SIZE)
        return json.loads(byte_message)

    def get(self, data):
        if self.initialized:
            return {"res": "FAILURE", "reason": "account already initialized"}
        msg = customer.send(Customer.SERVER_ADDR, data)
        if msg['res'] == 'SUCCESS':
            self.name = msg['data']['name']
            self.balance = float(msg['data']['balance'])
            self.cohort = msg['data']['cohort']

            for each in self.cohort:
                each["port2"] = int(each["port2"])
            self.initialized = True
        return msg

    def transfer(self, data):
        tokens = data.split()

        command = tokens[0]
        amount = int(tokens[1])
        recipient = tokens[2]
        label = int(tokens[3])

        if not self.initialized:
            return {"res": "FAILURE", "reason": "not initialized"}
        
        if not self.cohort:
            return {"res": "FAILURE", "reason": "no cohort"}

        for each in self.cohort:
            if each["name"] == recipient:
                ipv4 = each["ipv4"]
                port2 = each["port2"]

                msg = self.withdraw(amount)
                if msg["res"] == "FAILURE":
                    return msg
                return self.send((ipv4, port2), data)
        return {"res": "FAILURE", "reason": "recipient not found"}
    
    def listen_to_cohort(self):
        def helper():
            ipv4 = None
            port2 = None

            for each in self.cohort:
                if each['name'] == self.name:
                    ipv4 = each['ipv4']
                    port2 = each['port2']
                    break
                        
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # IP/UDP
            sock.bind((ipv4, port2))
            
            while True:
                data, addr = sock.recvfrom(Customer.BUFFER_SIZE)
                data = data.decode()
                response = None

                # read commands received from the socket
                try:
                    if data.startswith("transfer"):
                        response = self.transfer_recv(data, addr)
                    else:
                        response = {"res": "FAILURE"}
                except Exception as e:
                    response = {"res": "FAILURE"}

                # later save checkpoint to a file
                self.sock.sendto(json.dumps(response).encode(), addr)
            
        
        if not self.initialized:
            return {"res": "FAILURE", "reason": "not initialized"}
        
        extra_thread = threading.Thread(target=helper)
        extra_thread.start()
        

    def transfer_recv(self, data, addr):
        tokens = data.split()
        command = tokens[0]
        amount = int(tokens[1])

        msg = self.deposit(amount)
        if msg["res"] == "SUCCESS":
            del msg['balance']
        return msg

    def deposit(self, amount):
        if not self.initialized:
            return {"res": "FAILURE", "reason": "not initialized"}
        if amount <= 0:
            return {"res": "FAILURE", "reason": "amount is less than or equal to zero"}

        self.balance += amount
        return {"res": "SUCCESS", "balance": self.balance}

    def withdraw(self, amount):
        if not self.initialized:
            return {"res": "FAILURE", "reason": "not initialized"}
        if amount <= 0:
            return {"res": "FAILURE", "reason": "amount is less than or equal to zero"}

        self.balance -= amount
        if (self.balance < 0):
            self.balance += amount
            return {"res": "FAILURE", "reason": "account balance negative"}
        return {"res": "SUCCESS", "balance": self.balance}


if __name__ == "__main__":
    customer = Customer()

    while True:
        command = input("msg: ")
        msg = None
        #### iniate threads for peer to peer communication
        if command.startswith("get"):
            msg = customer.get(command)
        elif command.startswith("listen-to-cohort"):
            customer.listen_to_cohort()
        elif command.startswith("deposit"):
            tokens = command.split()
            amount = int(tokens[1])
            msg = customer.deposit(amount)
        elif command.startswith("withdraw"):
            tokens = command.split()
            amount = int(tokens[1])
            msg = customer.withdraw(amount)
        elif command.startswith("transfer"):
            msg = customer.transfer(command)
        else:
            msg = customer.send(Customer.SERVER_ADDR, command)
        
        if msg is not None:
            print(msg)
