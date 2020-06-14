#!/usr/bin/env python
import bank_pb2
import sys
import threading
import socket
from threading import Thread
from time import sleep
from random import randint
import random

balance = 0
branches_all = []
branch_soc_list = {}
mutex = threading.Lock()
this_branch = ''
curr_snap_id = 0
RECORD = {}
global_snapshot = {}
marker_count = 0
port = 0


def record_list():
    global RECORD
    global branches_all

    for obj in branches_all:
        RECORD[obj["port"]] = False


def branches_add(branch_list):
    for obj in branch_list:
        dictionary = {}
        for info in obj.ListFields():
            dictionary[info[0].name] = info[1]
        if dictionary["port"] != int(sys.argv[2]):
            branches_all.append(dictionary)
    record_list()


def connect_branches():
    print("connecting to other branches")
    try:
        for obj in branches_all:
            mutex.acquire()
            soc = create_soc(obj["ip"], obj["port"])
            branch_soc_list[obj["port"]] = soc
            mutex.release()
    except KeyboardInterrupt:
        soc.close()
        exit()


def create_soc(ip, port_local):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port_local))
    return s
    

def sleep_thread():
    global balance
    global branches_all
    global branch_soc_list
    global port

    t = float(sys.argv[3])
    while 1:
        print "Sleeping for", t/1000, "seconds"
        sleep(t/1000)
        print "Woke up"
        transfer_msg = bank_pb2.Transfer()
        withdraw = randint(10, 50)
        val = random.choice(branch_soc_list.keys())
        if withdraw <= balance:
            transfer_msg.money = withdraw
            transfer_msg.src_branch = sys.argv[2]
            transfer_msg.dst_branch = str(val)
            send_message = bank_pb2.BranchMessage()
            send_message.transfer.CopyFrom(transfer_msg)
            lock = threading.Lock()
            lock.acquire()
            if not RECORD[int(val)]:
                print "Sending", withdraw, "to", val
                balance -= withdraw
                branch_soc_list[val].sendall(send_message.SerializeToString())
                print "Remaining balance is:", balance
            else:
                print "Not sending to", val
            lock.release()


def receiver(client_socket):
    global balance
    global branches_all
    global branch_soc_list
    global curr_snap_id
    global this_branch
    global marker_count
    global RECORD

    while 1:
        received_message = client_socket.recv(1024)
        data = bank_pb2.BranchMessage()
        data.ParseFromString(received_message)
        if data.WhichOneof('branch_message') == 'init_branch':
            lock = threading.Lock()
            lock.acquire()
            balance = data.init_branch.balance
            lock.release()
            branches_add(data.init_branch.all_branches)
            connect_branches()
            sleep(5)
            print("calling sleep thread")
            try:
                th = Thread(target=sleep_thread, args=())
                th.daemon = True
                th.start()
            except KeyboardInterrupt:
                exit()
        if data.WhichOneof('branch_message') == 'transfer':

            if not RECORD[int(data.transfer.src_branch)]:
                lock = threading.Lock()
                lock.acquire()
                balance += data.transfer.money
                print "Balance in the bank after getting transferred money:", balance
                lock.release()
            else:
                print "Recording", data.transfer.src_branch, "->", this_branch, data.transfer.money
                lock = threading.Lock()
                lock.acquire()
                if curr_snap_id not in global_snapshot:
                    global_snapshot[curr_snap_id] = {}
                global_snapshot[curr_snap_id][data.transfer.src_branch] = data.transfer.money
                lock.release()
        if data.WhichOneof('branch_message') == 'init_snapshot':
            print "Received init_snapshot for", this_branch, "with snap_id", data.init_snapshot.snapshot_id
            lock = threading.Lock()
            lock.acquire()
            for obj in RECORD:
                RECORD[obj] = True
            curr_snap_id = data.init_snapshot.snapshot_id
            if curr_snap_id not in global_snapshot:
                global_snapshot[curr_snap_id] = {}
            global_snapshot[curr_snap_id]['balance'] = balance
            lock.release()
            for banks in branch_soc_list:
                marker = bank_pb2.Marker()
                marker.src_branch = str(port)
                marker.dst_branch = str(banks)
                marker.snapshot_id = curr_snap_id
                send_message = bank_pb2.BranchMessage()
                send_message.marker.CopyFrom(marker)
                print "Sending marker to", banks, "for snapshot", curr_snap_id
                branch_soc_list[banks].sendall(send_message.SerializeToString())
        if data.WhichOneof('branch_message') == 'marker':
            if data.marker.snapshot_id != curr_snap_id:
                print "Received marker from", data.marker.src_branch, "and snap_id is", data.marker.snapshot_id
                lock = threading.Lock()
                lock.acquire()
                for obj in RECORD:
                    RECORD[obj] = True
                curr_snap_id = data.marker.snapshot_id
                if curr_snap_id not in global_snapshot:
                    global_snapshot[curr_snap_id] = {}
                global_snapshot[curr_snap_id]['balance'] = balance
                lock.release()
                for banks in branch_soc_list:
                    marker = bank_pb2.Marker()
                    marker.src_branch = str(port)
                    marker.dst_branch = str(banks)
                    marker.snapshot_id = curr_snap_id
                    send_message = bank_pb2.BranchMessage()
                    send_message.marker.CopyFrom(marker)
                    print "Sending marker to", banks, "for snapshot", curr_snap_id
                    branch_soc_list[banks].sendall(send_message.SerializeToString())
                    if banks == data.marker.src_branch:
                        RECORD[banks] = False
            elif marker_count < len(branches_all):
                print "Reply marker from", data.marker.src_branch, "for snapshot", data.marker.snapshot_id
                marker_count += 1
                lock = threading.Lock()
                lock.acquire()
                RECORD[int(data.marker.src_branch)] = False
                if marker_count == len(branches_all):
                    print "Received markers from all. Snapshot created"
                    marker_count = 0
                lock.release()
        if data.WhichOneof('branch_message') == 'retrieve_snapshot':
            print "Received retrieve_snapshot for", this_branch
            snapshot = bank_pb2.ReturnSnapshot()
            snapshot.local_snapshot.snapshot_id = data.retrieve_snapshot.snapshot_id
            for key, val in global_snapshot.items():
                if key == data.retrieve_snapshot.snapshot_id:
                    for key2, val2 in val.items():
                        if key2 != 'balance':
                            snapshot.local_snapshot.channel_state.append(int(str(key2)+str(val2)))
                        else:
                            snapshot.local_snapshot.balance = val2
            send_message = bank_pb2.BranchMessage()
            send_message.return_snapshot.CopyFrom(snapshot)
            client_socket.sendall(send_message.SerializeToString())


def client_handler(client_socket):
    try:
        th = Thread(target=receiver, args=([client_socket]))
        th.daemon = True
        th.start()
    except KeyboardInterrupt:
        exit()

            
def main():
    global this_branch
    global port
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host = socket.gethostname()
    this_branch = sys.argv[1]
    port = int(sys.argv[2])
    sock.bind((host, port))
    sock.listen(1)
    try:
        while True:
            client_socket, client_address = sock.accept()
            print("Connected to client: ", client_address)
            t = Thread(target=client_handler, args=([client_socket]))
            t.daemon = True
            t.start()
    except KeyboardInterrupt:
        sock.close()
        exit()


main()
