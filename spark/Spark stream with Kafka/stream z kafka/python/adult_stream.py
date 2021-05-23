from socket import *
import time

adult = list()
with open("./adult.data", "r") as ad:
    for line in ad:
        adult.append(line)

HOST = "localhost"
PORT = 9998
ADDR = (HOST, PORT)
tcpSock = socket(AF_INET, SOCK_STREAM)
tcpSock.bind(ADDR)
tcpSock.listen(5)


while True:
    c, addr = tcpSock.accept()
    print("got connection")
    for line in adult:
        try:
            c.send(line.encode())
            time.sleep(1)
        except:
            break
    c.close()
    print("disconnected")
