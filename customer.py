import socket
import math
import json
import threading
from threading import Lock
import uuid

class CheckpointAndRollback:
    def __init__(self, customer) -> None:
        self.labels = self.initialize_labels(customer.name, customer.cohort)
        self.check_cohort = None
        self.willing_to_checkpoint = True
        self.myName = customer.name
        self.customer = customer

        self.has_tentative_checkpoint = False
        self.permanent_checkpoint = False
        
        #ready_to_roll = True
        #willing_to_rollback = True

    def initialize_labels(self, myName, cohort):
        labels = {}
        for each in cohort:
            if each['name'] != myName:
                other_client_name = each['name']
                labels[other_client_name] = Label()
        return labels
    

    def initialize_check_cohort(self):
        check_cohort = {}

        for other_client_name, label in self.labels.items():
            if label.last_recv > 0:
                check_cohort[other_client_name] = label
        return check_cohort

    def get_ipv4_and_port(self, other_client_name):
        for each in self.customer.cohort:
            if each["name"] == other_client_name:
                addr = each["ipv4"]
                port2 = each["port2"]
                return (addr, port2)
        return (None, None)

    def send_take_a_tentative_checkpoint(self):
        cmd = "take-a-tentative-checkpoint"

        answers = []
        for other_client_name in self.check_cohort:
            ipv4_port2 = self.get_ipv4_and_port(other_client_name)
            last_label_recvd = self.labels[other_client_name].last_recv

            msg = self.customer.send(ipv4_port2, f"{cmd} {self.myName} {last_label_recvd} {self.checkpoint_id}")
            if msg["res"] == "SUCCESS":
                answers.append(True)
            else:
                answers.append(False)

        return all(answers)

    def send_make_tentative_check_permanent(self):
        cmd = "make-tentative-checkpoint-permanent"
        answers = []
        for other_client_name in self.check_cohort:
            ipv4_port2 = self.get_ipv4_and_port(other_client_name)
            
            msg = self.customer.send(ipv4_port2, f"{cmd} {self.checkpoint_id}")
            if msg["res"] == "SUCCESS":
                answers.append(True)
            else:
                answers.append(False)

        # If false, an error happened
        return all(answers)

    def send_undo_tentative_checkpoint(self):
        cmd = "undo-tentative-checkpoint"
        answers = []
        for other_client_name in self.check_cohort:
            ipv4_port2 = self.get_ipv4_and_port(other_client_name)
            
            msg = self.customer.send(ipv4_port2, f"{cmd} {self.checkpoint_id}")
            if msg["res"] == "SUCCESS":
                answers.append(True)
            else:
                answers.append(False)

        # If false, an error happened
        return all(answers)

    def checkpoint(self):
        self.check_cohort = self.initialize_check_cohort()
        self.checkpoint_id = str(uuid.uuid4())
        self.has_tentative_checkpoint = True
        all_success = self.send_take_a_tentative_checkpoint()
        
        if all_success:
            self.send_make_tentative_check_permanent()
        else:
            self.send_undo_tentative_checkpoint()

    def recv_take_a_tentative_checkpoint(self, data):
        tokens = data.split()
        command = tokens[0]
        initializer = tokens[1]
        last_label_rcvd = int(tokens[2])
        parent_checkpoint_id = tokens[3]

        if self.has_tentative_checkpoint:
            if self.checkpoint_id != parent_checkpoint_id:
                return {"res": "FAILURE", "reason": "different checkpoint id"}
            else:
                return {"res": "SUCCESS"}

        if self.labels[initializer].first_sent != last_label_rcvd:
            self.willing_to_checkpoint = False

        if self.willing_to_checkpoint and (last_label_rcvd >= self.labels[initializer].first_sent > 0):
            self.has_tentative_checkpoint = True
            all_success = self.send_take_a_tentative_checkpoint()
            if all_success:
                return {"res": "SUCCESS"}
            else:
                return {"res": "FAILURE", "reason": "members of checkpoint cohort not willing to checkpoint"} 
        
    def recv_make_tentative_checkpoint_permanent(self, data):
        tokens = data.split()
        command = tokens[0]
        parent_checkpoint_id = tokens[1]

        if not self.has_tentative_checkpoint:
            return {"res": "FAILURE", "reason": "client does not have tentative checkpoint"}
        if parent_checkpoint_id != self.checkpoint_id:
            return {"res": "FAILURE", "reason": "different checkpoint id"}
        
        self.permanent_checkpoint = True
        self.has_tentative_checkpoint = False
        all_success = self.send_make_tentative_check_permanent()
        if all_success:
            return {"res": "SUCCESS"}
        else:
            return {"res": "FAILURE", "reason": "members of checkpoint cohort not willing to make tentative checkpoint permanent"} 

    def recv_undo_tentative_checkpoint(self,data):
        tokens = data.split()
        command = tokens[0]
        parent_checkpoint_id = tokens[1]

        if not self.has_tentative_checkpoint:
            return {"res": "FAILURE", "reason": "client does not have tentative checkpoint"}
        if parent_checkpoint_id != self.checkpoint_id:
            return {"res": "FAILURE", "reason": "different checkpoint id"}
        
        self.has_tentative_checkpoint = False
        self.permanent_checkpoint = False
        all_success = self.send_undo_tentative_checkpoint()
        if all_success:
            return {"res": "SUCCESS"}
        else:
            return {"res": "FAILURE", "reason": "members of checkpoint cohort not willing to undo tentative checkpoint"} 


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

        self.balance_lock = Lock()

        self.chk_rollback = None

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
            self.chk_rollback = CheckpointAndRollback(self)

            for each in self.cohort:
                each["port2"] = int(each["port2"])
                if each['name'] != self.name:
                    other_client_name = each['name']
                    self.chk_rollback.labels[other_client_name] = Label()

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

                self.chk_rollback.labels[recipient].first_sent = label
                self.chk_rollback.labels[recipient].last_sent = label

                if not emulateLostTransfer:
                    data += f" {self.name}"
                    msg = self.send((ipv4, port2), data)
                    if msg["res"] == "FAILURE":
                        self.deposit(amount)
                    else:
                        return msg

        return {"res": "FAILURE", "reason": "recipient not found"}

    def checkpoint(self):
        if self.chk_rollback and self.chk_rollback.permanent_checkpoint == True:
            return {"res": "FAILURE", "reason": "there is already running checkpoint"}

        
        msg = self.chk_rollback.checkpoint()
        self.chk_rollback = CheckpointAndRollback(self)
        return msg

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
                    elif data.startswith("take-a-tentative-checkpoint"):
                        self.chk_rollback.recv_take_a_tentative_checkpoint(data)
                    elif data.startswith("make-tentative-checkpoint-permanent"):
                        self.chk_rollback.recv_make_tentative_checkpoint_permanent(data)
                    elif data.startswith("undo-tentative-checkpoint"):
                        self.chk_rollback.recv_undo_tentative_checkpoint(data)
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
            self.chk_rollback.labels[sender].last_recv += 1
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
        elif command.startswith("checkpoint"):
            msg = customer.checkpoint()
        else:
            msg = customer.send(Customer.SERVER_ADDR, command)

        if msg is not None:
            print(msg)
