import socket
import random
from ipaddress import ip_address, IPv4Address
import csv
import json


def read_csv_file(filepath):
    data = []

    with open(filepath) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                data.append(row)
                line_count += 1
    return data


class Bank:

    CUSTOMER_FILE_NAME = "customers.csv"

    BUFFER_SIZE = 1024
    IP = "0.0.0.0"
    PORT = 5000

    customers = []      # a list of customers
    cohorts = []        # a list of all cohorts

    def __init__(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # IP/UDP
        sock.bind((Bank.IP, Bank.PORT))

        self.sock = sock
        self.customers = read_csv_file(Bank.CUSTOMER_FILE_NAME)

    def run(self):
        while True:
            data, addr = self.sock.recvfrom(1024)
            data = str(data)
            response = None

            if data.startswith("open"):
                resonse = self.open(data, addr)
            elif data.startswith("new-cohort"):
                response = self.new_cohort(data, addr)
            elif data.startswith("delete-cohort"):
                response = self.delete_cohort(data, addr)
            elif data.startswith("exit"):
                response = self.exit(data, addr)
            else:
                response = {"res": "FAILURE"}

            self.socket.sendto(json.dumps(response), addr)

    def open(self, data, addr):
        # tokenize the command string, excluding the first parameter
        tokens = data.split(' ')[1:]

        name = tokens[0]
        balance = tokens[1]
        address = tokens[2]
        server_port = tokens[3]
        client_port = tokens[4]

        def validIP(address: str):
            return True if type(ip_address(address)) is IPv4Address else False

        if (len(name) > 15 or (server_port == client_port) or validIP(address)):
            return {"res": "FAILURE"}

        # Add customer information to database
        if (tuple(tokens) not in Bank.customers):
            Bank.customers.append(tuple(tokens))
            return {"res": "SUCCESS"}
        else:
            return {"res": "FAILURE"}

    def new_cohort(self, data, addr):
        tokens = data.split(' ')

        command = tokens[0]
        customer = tokens[1]
        n = tokens[2]

        if (n > len(Bank.customers)):
            return {"res": "FAILURE"}

        new_group = [customer]  # new cohort group
        res = []
        customers_without_group = set()

        for customer in self.customers:
            name = customer[0]
            for group in self.cohorts:
                if name not in group:       # empty intersection
                    customers_without_group.add(customer)

        picked_customers = random.choice(customers_without_group, n-1)

        for customer in picked_customers:
            res.append(customer)
            # add the name of the customer to the cohort group
            new_group.append(customer[0])

        self.cohorts.append(new_group)
        return {
            "res": "SUCCESS",
            "data": res
        }

    def delete_cohort(self, data, addr):
        pass

    def exit(self, data, addr):
        pass


if __name__ == "__main__":
    bank = Bank()
    bank.run()
