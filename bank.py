import socket

class Bank:
    BUFFER_SIZE = 1024
    IP = "0.0.0.0"
    PORT = 5000

    def __init__(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # IP/UDP
        sock.bind((Bank.IP, Bank.PORT))

    def run(self):
        while True:
            data, addr = sock.recvfrom(1024)
            
            if data.startswith("open"):
                self.open(data, addr)
            elif data.startswith("new-cohort"):
                self.new_cohort(data, addr)
            elif data.startswith("delete-cohort"):
                self.delete_cohort(data, addr)
            elif data.startswith("exit"):
                self.exit(data, addr)
            else:
                # throw error or send error message
                pass


    def open(data, addr):
        pass

    def new_cohort(data, addr):
        pass

    def delete_cohort(data, addr):
        pass

    def exit(data, addr):
        pass
