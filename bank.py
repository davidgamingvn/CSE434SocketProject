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


def read_cohort_number(filepath):
    with open(filepath) as cohort_file:
        return int(cohort_file.read())


def write_customers_file(li, filepath):
    with open(filepath, mode='w') as file:
        writer = csv.writer(file, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['#', 'Customer', 'Balance',
                        'IPv4 Address', 'Port1', 'Port2', 'Cohort'])

        for each in li:
            writer.writerow(each)


def write_cohort_number(number, filepath):
    with open(filepath, 'w') as file:
        file.write(number)


class Bank:

    CUSTOMER_FILE_NAME = "customers.csv"
    COHORT_NUMBER_FILE_NAME = "cohort_number.txt"

    BUFFER_SIZE = 1024
    IP = "0.0.0.0"
    PORT = 5000

    def __init__(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # IP/UDP
        sock.bind((Bank.IP, Bank.PORT))

        self.sock = sock
        self.customers = read_csv_file(Bank.CUSTOMER_FILE_NAME)
        self.cohort_number = read_cohort_number(Bank.COHORT_NUMBER_FILE_NAME)

    def run(self):
        while True:
            data, addr = self.sock.recvfrom(1024)
            data = data.decode()
            response = None
            try:
                if data.startswith("open"):
                    response = self.open(data, addr)
                elif data.startswith("new-cohort"):
                    response = self.new_cohort(data, addr)
                elif data.startswith("delete-cohort"):
                    response = self.delete_cohort(data, addr)
                elif data.startswith("exit"):
                    response = self.exit(data, addr)
                else:
                    response = {"res": "FAILURE"}
            except Exception as e:
                response = {"res": "FAILURE"}

            # write to files
            write_customers_file(self.customers, Bank.CUSTOMER_FILE_NAME)
            write_cohort_number(self.cohort_number,
                                Bank.COHORT_NUMBER_FILE_NAME)

            self.sock.sendto(json.dumps(response).encode(), addr)

    def open(self, data, addr):
        # tokenize the command string, excluding the first parameter
        tokens = data.split(' ')[1:]

        name = tokens[0]
        balance = tokens[1]
        address = tokens[2]
        server_port = tokens[3]
        client_port = tokens[4]

        def validIP(address: str) -> bool:
            return True if type(ip_address(address)) is IPv4Address else False

        if (len(name) > 15 or (server_port == client_port) or not validIP(address)):
            return {"res": "FAILURE"}

        # Add customer information to database
        if (tokens not in Bank.customers):

            tokens.append('0')
            Bank.customers.append(tokens)
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

        res = []
        customers_without_cohort = []

        for c in self.customers:
            name = c[0]
            cohort = c[6]

            if cohort != 0:
                if name == customer:
                    break
                customers_without_cohort.append(c)

        if len(customers_without_cohort) < n:
            return {"res": "FAILURE"}

        picked_customers = random.choice(customers_without_cohort, n-1)

        for c in picked_customers:
            c[6] = self.cohort_number
            self.cohort_number += 1
            res.append(c)

        return {
            "res": "SUCCESS",
            "data": res
        }

    def delete_cohort(self, data, addr):
        success_response = {"res": "SUCCESS"}
        failure_response = {"res": "FAILURE"}
        tokens = data.split()

        if len(tokens) != 2:
            return failure_response

        command, customer = tokens
        has_group = False
        customer_cohort = 0

        for c in self.customers:
            name = c[0]
            cohort = c[6]

            if name == customer and cohort != 0:
                has_group = True
                customer_cohort = cohort
                break

        if not has_group:
            return failure_response

        for c in self.customers:
            cohort = c[6]

            if cohort == customer_cohort:
                c[6] = 0

        return success_response

    def exit(self, data, addr):
        success_response = {"res": "SUCCESS"}
        failure_response = {"res": "FAILURE"}

        tokens = data.split()
        command, customer = tokens
        user_exists = False

        if len(tokens) != 2:
            return failure_response

        for i, c in enumerate(self.customers):
            name = c[0]

            if name == customer:
                user_exists = True
                break

        if user_exists:
            return success_response
        else:
            return failure_response


if __name__ == "__main__":
    bank = Bank()
    bank.run()
