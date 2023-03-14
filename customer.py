import socket
import math
import json
import threading
from threading import Lock

class CheckpointAndRollback:
    def __init__(self, myName, cohort) -> None:
        self.labels = self.initialize_labels(myName, cohort)
        self.willing_to_checkpoint = True
        
        #ready_to_roll = True
        #willing_to_rollback = True

    def initialize_labels(self, myName, cohort):
        labels = {}
        for each in cohort:
            if each['name'] != myName:
                other_client_name = each['name']
                labels[other_client_name] = Label()
        return labels
    
    def checkpoint(self, customer):
        check_cohort = {}

        for other_client_name, label in self.labels.items():
            if label.last_recv > 0:
                check_cohort[other_client_name] = label
        

        answers = []
        for other_client_name in self.labels:
            info = customer.cohort[other_client_name]

            addr = info["ipv4"]
            port2 = info["port2"]
            ans = customer.send((addr, port2), "tentative")
            answers.append(ans)
        
        for other_client_name in self.labels:
            info = customer.cohort[other_client_name]

            addr = info["ipv4"]
            port2 = info["port2"]

            if all(answers):
                customer.send((addr, port2), "make-permanent")
            else:
                customer.send((addr, port2), "undo")

    def checkpoint_recv(self, data):
        pass


class Label:
    def __init__(self) -> None:
        self.first_sent = 0
        self.last_sent = 0
        self.last_recv = 0

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
        self.labels = {}

        self.balance_lock = Lock()

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
                if each['name'] != self.name:
                    other_client_name = each['name']
                    self.labels[other_client_name] = Label()

            self.initialized = True
        return msg

    def transfer(self, data, emulateLostTransfer=False):
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

                self.labels[recipient].last_sent = label

                if not emulateLostTransfer:
                    data += f" {self.name}"
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
        recipient = tokens[2]
        label = int(tokens[3])
        sender = tokens[4]

        msg = self.deposit(amount)

        if msg["res"] == "SUCCESS":
            self.labels[sender].last_recv += 1
            del msg['balance']
        return msg

    def deposit(self, amount):
        if not self.initialized:
            return {"res": "FAILURE", "reason": "not initialized"}
        if amount <= 0:
            return {"res": "FAILURE", "reason": "amount is less than or equal to zero"}

        with self.balance_lock:
            self.balance += amount
        return {"res": "SUCCESS", "balance": self.balance}

    def withdraw(self, amount):
        if not self.initialized:
            return {"res": "FAILURE", "reason": "not initialized"}
        if amount <= 0:
            return {"res": "FAILURE", "reason": "amount is less than or equal to zero"}
        with self.balance_lock:
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
        # iniate threads for peer to peer communication
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
        elif command.startswith("lost-transfer"):
            msg = customer.transfer(command, emulateLostTransfer=True)
        else:
            msg = customer.send(Customer.SERVER_ADDR, command)

        if msg is not None:
            print(msg)
