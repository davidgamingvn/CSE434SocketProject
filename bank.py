import socket
import random
from ipaddress import ip_address, IPv4Address
import csv
import json
import math

## open a new csv file, acting as a database
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

## open a text file  to read current cohort number
def read_cohort_number(filepath):
    with open(filepath) as cohort_file:
        return int(cohort_file.read())


## write a list of customers information to the csv database
def write_customers_file(li, filepath):
    with open(filepath, mode='w') as file:
        writer = csv.writer(file, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['Customer', 'Balance',
                        'IPv4 Address', 'Port1', 'Port2', 'Cohort'])

        for each in li:
            writer.writerow(each)

## write the current cohort number
def write_cohort_number(number, filepath):
    with open(filepath, 'w') as file:
        file.write(str(number))


class Bank:

    CUSTOMER_FILE_NAME = "customers.csv"
    COHORT_NUMBER_FILE_NAME = "cohort_number.txt"

    ## assigned port ranges
    GROUP_NUMBER = 39
    PORT_START = math.ceil(GROUP_NUMBER / 2) * 1000 + 500
    PORT_END = math.ceil(GROUP_NUMBER / 2) * 1000 + 999

    BUFFER_SIZE = 1024
    IP = "0.0.0.0"
    PORT = PORT_START

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
            
            ## read commands received from the socket
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

            # if the response has a success status code, update relevant text files.
            if response['res'] == 'SUCCESS':
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

        def validIP(address: str) -> bool:  #check if address field in the command is in correct ipv4 format
            return True if type(ip_address(address)) is IPv4Address else False

        # if there are more than one customer in the same host, check if they are using different port number

        if (len(name) > 15 or (server_port == client_port) or not validIP(address)):
            return {"res": "FAILURE"}

        # if the customer already exists in the database
        for c in self.customers:
            if name == c[0]:
                return {"res": "FAILURE"}

        # Add customer information to database
        if (tokens not in self.customers):

            tokens.append('0')  # add a cohort number field to the end of the customer information
            self.customers.append(tokens)
            return {"res": "SUCCESS"}
        else:
            return {"res": "FAILURE"}

    def new_cohort(self, data, addr):
        tokens = data.split(' ')

        command = tokens[0]
        customer = tokens[1]
        n = int(tokens[2])

        if n < 2:
            return {"res": "FAILURE"}

        if (n > len(self.customers)):
            return {"res": "FAILURE"}

        res = []
        customers_without_cohort = []   # temporary array to store any customers that are not already in a cohort

        for c in self.customers:
            name = c[0]
            cohort = c[5]

            if name == customer:    # if the customer is in the database but already has a cohort
                if int(cohort) != 0:
                    return {"res": "FAILURE"}
                else:
                    res.append(c)
            else:
                if int(cohort) == 0:    # if the customer is in the database and doesn't have a cohort
                    customers_without_cohort.append(c)

        if len(customers_without_cohort) < n - 1:
            return {"res": "FAILURE"}

        picked_customers = random.choices(customers_without_cohort, k=n-1)  # randomly choose a list of customers without cohorts

        for c in picked_customers:  # append the picked customers to the new cohort
            res.append(c)

        for c in res:
            c[5] = self.cohort_number   # update the cohort number, incremented from the latest exisiting cohort number
        self.cohort_number += 1

        return {
            "res": "SUCCESS",
            "data": res
        }

    # delete a cohort group that a member belongs to
    # as the cohort gorup is deleted, other members who
    # are in the gorup are affected
    # params:
    #   data: string type, command given by client
    #         string format should be "delete-cohort <customer>"
    #   addr: tuple of connected client's addr and port
    # return:
    #   on success: {"res": "SUCCESS"}
    #   on failure: {"res": "FAILURE"}
    def delete_cohort(self, data, addr):
        success_response = {"res": "SUCCESS"}
        failure_response = {"res": "FAILURE"}
        tokens = data.split()

        if len(tokens) != 2:
            return failure_response

        command, customer = tokens
        has_group = False
        customer_cohort = 0

        # find if the member exists and he/she is
        # in cohort
        for c in self.customers:
            name = c[0]
            cohort = c[5]

            if name == customer and cohort != 0:
                has_group = True
                customer_cohort = cohort
                break

        # member not exists or member not belongs to cohort
        if not has_group:
            return failure_response

        # removes other members from the same cohort
        for c in self.customers:
            cohort = c[5]

            if cohort == customer_cohort:
                c[5] = 0

        return success_response

    # delete a member info from database
    # if the member is already in cohort, this method delete
    # the cohort first by using "delete_cohort" method
    # params:
    #       data: string type, command given by client
    #             string format is "exit <customer>""
    #       addr: tuple of connected client's addr and port
    # return:
    #   on success: {"res": "SUCCESS"}
    #   on failure: {"res": "FAILURE"}
    def exit(self, data, addr):
        success_response = {"res": "SUCCESS"}
        failure_response = {"res": "FAILURE"}

        tokens = data.split()
        command, customer = tokens
        user_exists = False

        self.delete_cohort(f"delete-cohort {customer}", addr)

        if len(tokens) != 2:
            return failure_response

        # check if user exists
        for i, c in enumerate(self.customers):
            name = c[0]

            if name == customer:
                user_exists = True
                break

        if user_exists:
            self.customers.pop(i)
            return success_response
        else:
            return failure_response


if __name__ == "__main__":
    bank = Bank()
    bank.run()
