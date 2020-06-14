import bank_pb2
import sys
import socket
from time import sleep
import random

if __name__ == '__main__':
    s_id = 1
    amount = int(sys.argv[1])
    fn = sys.argv[2]
    b = []
    branchSocList = {}
    port_map = {}
    with open(fn) as f:
        for line in f:
            b.append(line.split())
    try:
        bal = int(amount / len(b))
        for branch in b:
            port_map[branch[2]] = branch[0]
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((branch[1], int(branch[2])))
            initMsg = bank_pb2.InitBranch()
            initMsg.balance = bal
            for banks in b:
                branches = initMsg.all_branches.add()
                branches.name = banks[0]
                branches.ip = banks[1]
                branches.port = int(banks[2])
            send_Message = bank_pb2.BranchMessage()
            send_Message.init_branch.CopyFrom(initMsg)
            s.sendall(send_Message.SerializeToString())
            branchSocList[branch[2]] = s
        while 1:
            sleep(10)
            initSnapshot = bank_pb2.InitSnapshot()
            initSnapshot.snapshot_id = s_id
            val = random.choice(branchSocList.keys())
            print 'Initiating snapshot for:', val
            send_message = bank_pb2.BranchMessage()
            send_message.init_snapshot.CopyFrom(initSnapshot)
            branchSocList[val].sendall(send_message.SerializeToString())
            sleep(10)
            retrieveSnapshot = bank_pb2.RetrieveSnapshot()
            retrieveSnapshot.snapshot_id = s_id
            send_message2 = bank_pb2.BranchMessage()
            send_message2.retrieve_snapshot.CopyFrom(retrieveSnapshot)
            print "snapshot_id:", s_id
            for banks in branchSocList:
                filled = {}
                for obj in port_map:
                    filled[obj] = 0
                branchSocList[banks].sendall(send_message2.SerializeToString())
                received_message = branchSocList[banks].recv(1024)
                data = bank_pb2.BranchMessage()
                data.ParseFromString(received_message)
                if data.WhichOneof('branch_message') == 'return_snapshot':
                    localSnap = data.return_snapshot.local_snapshot
                    # print localSnap
                    print port_map[banks], ":", localSnap.balance,
                    for val in localSnap.channel_state:
                        for val2 in port_map:
                            if str(val).startswith(str(val2)):
                                filled[val2] = str(val)[len(str(val2)):]
                                # print port_map[str(val2)], "->", port_map[banks], ":", str(val)[len(str(val2)):]
                    for branch in filled:
                        if branch != banks:
                            print port_map[branch], "->", port_map[banks], ":", filled[branch],
                print
            s_id += 1

    except KeyboardInterrupt:
        print ("\n Server Stopped----\n")
