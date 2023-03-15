import socket
import math
import json
import threading
from threading import Lock
import uuid
import csv
import sys


class CheckpointAndRollback:
    CHECKPOINT_FILE_NAME = "checkpoint.csv"

    def __init__(self, customer) -> None:
        self.labels = self.initialize_labels(customer.name, customer.cohort)
        self.check_cohort = None
        self.myName = customer.name
        self.customer = customer

        self.willing_to_checkpoint = True
        self.has_tentative_checkpoint = False
        self.permanent_checkpoint = False
        self.executed_make_permanent_checkpoint = False

        self.resume_execution = True
        self.willing_to_rollback = True
        self.roll_cohort = self.labels

        self.has_prepare_rollback = False
        self.permanent_rollback = False
        self.executed_make_permanent_rollback = False

    def write_checkpoint_to_file(self):
        with open(f"{self.myName}_" + self.CHECKPOINT_FILE_NAME, mode='w', newline='') as file:
            writer = csv.writer(file, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(['Customer', 'Balance', 'IPv4 Address', 'Port2'])

            # write first row that is owner of this client
            for each in self.customer.cohort:
                if each['name'] == self.myName:
                    writer.writerow([self.myName, str(self.customer.balance), str(
                        each['ipv4']), str(each['port2'])])
                    break

            # for the rest of the file, write data of customers in the same cohort
            for each in self.customer.cohort:
                if each['name'] != self.myName:
                    writer.writerow(
                        [str(each["name"]), str(-1), str(each['ipv4']), str(each['port2'])])


    def rollback_from_file(self):
        pass

    def initialize_labels(self, myName, cohort):
        labels = {}
        for each in cohort:
            if each['name'] != myName:
                other_client_name = each['name']
                labels[other_client_name] = Label()
        return labels

    def update_check_cohort(self):
        check_cohort = {}

        for other_client_name, label in self.labels.items():
            if label.last_recv > 0:
                check_cohort[other_client_name] = label
        self.check_cohort = check_cohort

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

        self.update_check_cohort()
        for other_client_name in self.check_cohort:
            ipv4_port2 = self.get_ipv4_and_port(other_client_name)
            last_label_recvd = self.labels[other_client_name].last_recv

            msg = self.customer.send(
                ipv4_port2, f"{cmd} {self.myName} {last_label_recvd} {self.checkpoint_id}")
            if msg["res"] == "SUCCESS":
                answers.append(True)
            else:
                answers.append(False)

        return all(answers)

    def send_make_tentative_check_permanent(self):
        cmd = "make-tentative-checkpoint-permanent"
        answers = []

        self.executed_make_permanent_checkpoint = True

        self.update_check_cohort()
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

        self.update_check_cohort()
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
        self.update_check_cohort()
        self.checkpoint_id = str(uuid.uuid4())
        self.has_tentative_checkpoint = True
        all_success = self.send_take_a_tentative_checkpoint()

        if not all_success:
            self.send_undo_tentative_checkpoint()
            return {"res": "FAILURE", "reason": "take a tentative checkpoint failed"}

        all_success = self.send_make_tentative_check_permanent()
        if all_success:
            self.write_checkpoint_to_file()
            return {"res": "SUCCESS"}
        else:
            return {"res": "FAILURE", "reason": "make tentative checkpoint permanent failed"}

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
        self.checkpoint_id = parent_checkpoint_id

        value_without_offset = (
            last_label_rcvd + self.labels[initializer].base - 1)

        if self.labels[initializer].first_sent != value_without_offset:
            self.willing_to_checkpoint = False

        if self.willing_to_checkpoint and (value_without_offset >= self.labels[initializer].first_sent > 0):
            self.has_tentative_checkpoint = True
            all_success = self.send_take_a_tentative_checkpoint()
            if all_success:
                return {"res": "SUCCESS"}
            else:
                return {"res": "FAILURE", "reason": "members of checkpoint cohort not willing to checkpoint"}

        return {"res": "FAILURE", "reason": "members of checkpoint cohort not willing to checkpoint"}

    def recv_make_tentative_checkpoint_permanent(self, data):
        tokens = data.split()
        command = tokens[0]
        parent_checkpoint_id = tokens[1]

        if not self.has_tentative_checkpoint:
            return {"res": "FAILURE", "reason": "client does not have tentative checkpoint"}
        if parent_checkpoint_id != self.checkpoint_id:
            return {"res": "FAILURE", "reason": "different checkpoint id"}
        if self.executed_make_permanent_checkpoint == True:
            return {"res": "SUCCESS"}

        self.permanent_checkpoint = True
        self.has_tentative_checkpoint = False
        all_success = self.send_make_tentative_check_permanent()
        if all_success:
            self.write_checkpoint_to_file()
            self.customer.chk_rollback = CheckpointAndRollback(self.customer)
            return {"res": "SUCCESS"}
        else:
            return {"res": "FAILURE", "reason": "members of checkpoint cohort not willing to make tentative checkpoint permanent"}

    def recv_undo_tentative_checkpoint(self, data):
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

    def send_prepare_to_rollback(self):
        cmd = "prepare-to-rollback"
        answers = []

        for other_client_name in self.roll_cohort:
            ipv4_port2 = self.get_ipv4_and_port(other_client_name)
            last_label_sent = self.labels[other_client_name].last_sent

            msg = self.customer.send(
                ipv4_port2, f"{cmd} {self.myName} {last_label_sent} {self.rollback_id}")
            if msg["res"] == "SUCCESS":
                answers.append(True)
            else:
                answers.append(False)

        return all(answers)

    def send_rollback(self):
        cmd = "send-rollback"
        answers = []

        self.executed_make_permanent_rollback = True

        for other_client_name in self.roll_cohort:
            ipv4_port2 = self.get_ipv4_and_port(other_client_name)

            msg = self.customer.send(ipv4_port2, f"{cmd} {self.rollback_id}")
            if msg["res"] == "SUCCESS":
                answers.append(True)
            else:
                answers.append(False)

        # If false, an error happened
        return all(answers)

    def send_do_not_rollback(self):
        cmd = "do-not-rollback"
        answers = []

        for other_client_name in self.roll_cohort:
            ipv4_port2 = self.get_ipv4_and_port(other_client_name)

            msg = self.customer.send(ipv4_port2, f"{cmd} {self.rollback_id}")
            if msg["res"] == "SUCCESS":
                answers.append(True)
            else:
                answers.append(False)

        # If false, an error happened
        return all(answers)

    def rollback(self):
        self.rollback_id = str(uuid.uuid4())
        self.has_prepare_rollback = True
        all_success = self.send_prepare_to_rollback()

        if not all_success:
            self.send_do_not_rollback()
            self.has_prepare_rollback = False
            return {"res": "FAILURE", "reason": "send-prepare-to-rollback failed"}

        all_success = self.send_rollback()
        if all_success:
            # todo: restore instance from checkpoint file
            return {"res": "SUCCESS"}
        else:
            return {"res": "FAILURE", "reason": "send-rollback failed"}

    def recv_prepare_to_rollback(self, data):
        tokens = data.split()
        command = tokens[0]
        initializer = tokens[1]
        last_label_sent = int(tokens[2])
        parent_rollback_id = tokens[3]

        if self.has_prepare_rollback:
            if self.rollback_id != parent_rollback_id:
                return {"res": "FAILURE", "reason": "different rollback id"}
            else:
                return {"res": "SUCCESS"}

        self.rollback_id = parent_rollback_id
        
        value_without_offset = (
            self.labels[initializer].last_recv + self.labels[initializer].base - 1)

        if self.labels[initializer].first_sent == value_without_offset:
            self.willing_to_rollback = False

        self.has_prepare_rollback = True

        if self.willing_to_rollback and (value_without_offset > last_label_sent and self.resume_execution):
            self.has_prepare_rollback = True
            self.resume_execution = False

            all_success = self.send_prepare_to_rollback()
            if all_success:
                return {"res": "SUCCESS"}
            else:
                return {"res": "FAILURE", "reason": "inside recv_prepare_to_rollback(): send_prepare_to_rollback() failed"}

        #TODO: send rollback information, regardless of condition
        
        return {"res": "SUCCESS"}

    def recv_rollback(self, data):
        tokens = data.split()
        command = tokens[0]
        parent_rollback_id = tokens[1]

        if not self.has_prepare_rollback:
            return {"res": "FAILURE", "reason": "recv_rollback(): has_prepare_rollback is False"}
        if parent_rollback_id != self.rollback_id:
            return {"res": "FAILURE", "reason": "recv_rollback(): different rollback id"}
        if self.executed_make_permanent_rollback == True:
            return {"res": "SUCCESS"}

        self.permanent_rollback = True
        self.has_prepare_rollback = False
        self.executed_make_permanent_rollback = True
        all_success = self.send_rollback()
        if all_success:
            # TODO: get data from checkpoint
            # self.write_checkpoint_to_file()
            self.customer.chk_rollback = CheckpointAndRollback(self.customer)
            return {"res": "SUCCESS"}
        else:
            return {"res": "FAILURE", "reason": "recv_rollback(): send_rollback() failed"}

    def recv_do_not_rollback(self, data):
        tokens = data.split()
        command = tokens[0]
        parent_rollback_id = tokens[1]

        if not self.has_prepare_rollback:
            return {"res": "FAILURE", "reason": "recv_do_not_rollback(): has_prepare_rollback is False"}
        if parent_rollback_id != self.rollback_id:
            return {"res": "FAILURE", "reason": "recv_do_not_rollback(): different rollback id"}

        self.resume_execution = True
        self.has_prepare_rollback = False
        self.permanent_rollback = False
        all_success = self.send_do_not_rollback()
        if all_success:
            return {"res": "SUCCESS"}
        else:
            return {"res": "FAILURE", "reason": "recv_do_not_rollback(): send_do_not_rollback() failed"}


class Label:
    def __init__(self) -> None:
        self.base = 1
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

            # todo: need to initialize this part after checkpoint?
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

                if self.chk_rollback.labels[recipient].base is None:
                    self.chk_rollback.labels[recipient].base = label

                self.chk_rollback.labels[recipient].first_sent = label
                self.chk_rollback.labels[recipient].last_sent = label

                if not emulateLostTransfer:
                    data += f" {self.name}"
                    msg = self.send((ipv4, port2), data)
                    if msg["res"] == "FAILURE":
                        self.deposit(amount)
                    else:
                        return msg
                else:
                    return {"res": "SUCCESS", "emulate lost transfer": "True"}

        return {"res": "FAILURE", "reason": "recipient not found"}

    def checkpoint(self):
        msg = self.chk_rollback.checkpoint()
        if msg["res"] == "SUCCESS":
            self.chk_rollback = CheckpointAndRollback(self)
        return msg

    def rollback(self):
        msg = self.chk_rollback.rollback()
        if msg["res"] == "SUCCESS":
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
                        response = self.chk_rollback.recv_take_a_tentative_checkpoint(
                            data)
                    elif data.startswith("make-tentative-checkpoint-permanent"):
                        response = self.chk_rollback.recv_make_tentative_checkpoint_permanent(
                            data)
                    elif data.startswith("undo-tentative-checkpoint"):
                        response = self.chk_rollback.recv_undo_tentative_checkpoint(
                            data)
                    elif data.startswith("prepare-to-rollback"):
                        response = self.chk_rollback.recv_prepare_to_rollback(
                            data)
                    elif data.startswith("send-rollback"):
                        response = self.chk_rollback.recv_rollback(
                            data)
                    elif data.startswith("do-not-rolllback"):
                        response = self.chk_rollback.recv_do_not_rollback(
                            data)
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
        elif command.startswith("rollback"):
            msg = customer.rollback()
        else:
            msg = customer.send(Customer.SERVER_ADDR, command)

        if msg is not None:
            print(msg)
