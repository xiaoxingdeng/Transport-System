import socket
import json
import time
import threading
import os
from enum import Enum
import sys

configdict = {}
SERVER_HOST = 'localhost'
BUFSIZE = 1024
sendmap = {}
window = 20
mutex = threading.Lock()


class Status(Enum):
    LISTEN = 0
    SYNSENT = 1
    SYNRECEIVED = 2
    ESTABLISHED = 3


def send(data, s, address):
    s.sendto(data, address)


def newthreadsend(data, s, address):
    sendthread = threading.Thread(target=send, args=(data, s, address))
    sendthread.start()


def open_config(filename):
    global configdict
    with open(filename, "r") as f:
        configdict = json.load(f)


def split_data(data, datadict):
    datadict["sequence"] = int.from_bytes(data[0:4], 'big')
    datadict["ack"] = int.from_bytes(data[4:8], 'big')
    datadict["flag"] = int.from_bytes(data[8:12], 'big')
    datadict["data"] = data[12:]


def formheader(sequencenumber, acknumber, flag):
    data = sequencenumber.to_bytes(4, 'big')
    data = data + acknumber.to_bytes(4, 'big')
    data = data + flag.to_bytes(4, 'big')
    return data


def openserver():
    SERVER_PORT = int(configdict['port'])
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_address = (SERVER_HOST, SERVER_PORT)
    s.bind(server_address)
    while True:
        data, address = s.recvfrom(BUFSIZE)
        datadict = {}
        split_data(data, datadict)
        if datadict["flag"] == 0:
            threadsend = threading.Thread(target=newrequest, args=(datadict, s, address))
            threadsend.start()
        if datadict["flag"] == 2:
            mutex.acquire()
            sendmap[address] = datadict["ack"]
            mutex.release()
        if datadict["flag"] == 4:
            mutex.acquire()
            sendmap[address] = -1
            mutex.release()


def newrequest(datadict, s, address):
    sendmap[address] = -2
    filename = datadict["data"].decode()
    f = open(filename, 'rb+')
    windowstart = 0
    filelength = os.path.getsize(filename)
    while sendmap[address] == -2:
        s.sendto(formheader(0, 0, 1) + filelength.to_bytes(4, 'big'), address)
        time.sleep(0.1)
    while windowstart < filelength and windowstart != -1:
        f.seek(windowstart, 0)
        if windowstart + window * 1012 <= filelength:
            windowdata = f.read(window * 1012)
        else:
            windowdata = f.read(filelength - windowstart)
        windowlen = len(windowdata)
        for i in range(window):
            sequencenum = windowstart + i * 1012
            if i * 1012 <= windowlen - 1:
                if i * 1012 + 1011 <= windowlen - 1:
                    data = windowdata[i * 1012:i * 1012 + 1012]
                else:
                    data = windowdata[i * 1012:]
                s.sendto(formheader(sequencenum, 0, 3) + data, address)
        time.sleep(0.03)
        old = windowstart
        mutex.acquire()
        windowstart = sendmap[address]
        mutex.release()
        if (windowstart - old) < 20 * 1012:
            print(str(windowstart)+"   "+str(20 * 1012-windowstart + old))


def openclient(filename, hostname, port):
    f = open(filename, 'wb')
    clientstatus = Status.SYNSENT
    ip = socket.gethostbyname(hostname)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    filelength = 0
    s.settimeout(0.3)
    # initiate the three handashke
    while True:
        try:  # first SYN message
            header = formheader(0, 0, 0)
            first_data = header + filename.encode()
            newthreadsend(first_data, s, (ip, port))
            # receive SYN-ACK message
            data, address = s.recvfrom(BUFSIZE)
            datadict = {}
            split_data(data, datadict)
            if clientstatus == Status.SYNSENT:
                if datadict["flag"] == 1:
                    filelength = int.from_bytes(datadict["data"], 'big')
                    print(filelength)
                    s.sendto(formheader(0, 0, 2), address)
                    clientstatus = Status.ESTABLISHED
            break
        except socket.timeout:
            time.sleep(3)
            continue
    latestack = 0
    s.settimeout(5)

    while True:
        # wait for
        data, address = s.recvfrom(BUFSIZE)
        datadict = {}
        split_data(data, datadict)
        if clientstatus == Status.ESTABLISHED:
            if datadict["flag"] == 1:
                newthreadsend(formheader(0, latestack, 4), s, address)
            if datadict["flag"] == 3:
                # ACK is the required one
                if datadict["sequence"] == latestack:
                    f.write(datadict["data"])
                    latestack = latestack + len(datadict["data"])
                    # the file has finished
                    if latestack == filelength:
                        print("finish")
                        newthreadsend(formheader(0, latestack, 4), s, address)
                        f.close()
                        break
                    else:
                        newthreadsend(formheader(0, latestack, 2), s, address)
                # Resend data
                elif datadict["sequence"] < latestack:
                    newthreadsend(formheader(0, datadict["sequence"] + len(datadict["data"]), 2), s, address)
                # former miss
                else:
                    newthreadsend(formheader(0, latestack, 2), s, address)


def searchfile(filename):
    peerinfo = configdict["peer_info"]
    for item in peerinfo:
        for file in item["content_info"]:
            if file == filename:
                return item["hostname"], item["port"]
    return None, None


if __name__ == '__main__':
    open_config(sys.argv[1])
    threadserver = threading.Thread(target=openserver)
    threadserver.start()
    while True:
        filename = input()
        hostname, port = searchfile(filename)
        if hostname and port:
            thread_client = threading.Thread(target=openclient, args=(filename, hostname, port))
            thread_client.start()
