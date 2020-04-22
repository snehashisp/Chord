import MessageExecutor
import json
import threading
import sys
import time

class Node():

    def __init__(self, port, maxnodes, replicate_factor = 0):
        self.message_executor = MessageExecutor.MessageExecutor(port, maxnodes, replicate_factor)
        self.replicate_factor = replicate_factor
        self.nodeid = self.message_executor._nodeid
        self.comm = self.message_executor._comm
        self._message_thread_status = False
        print("Node Initialized With ", self.nodeid, self.comm.getIpPort())
        # print(self.message_executor._finger_table._table)

    def initialize(self, ip, port):
        joinMessage = self.message_executor._message_creator.createJoin()
        routeJoinMessage = self.message_executor._message_creator.createRouteMessage(self.nodeid, joinMessage)
        # print("Joining Network with routing message \n", routeJoinMessage)
        self.comm.sendMessage(routeJoinMessage, ip, port)
        response = json.loads(self.comm.recvMessage())
        while response["type"] != "join_response":
            response = json.loads(self.comm.recvMessage())
        # print("Got Response ", response)
        self.message_executor.joinResponse(response)
        print("Successfully joined network")
        # print(self.message_executor._finger_table._table)


    def _messageThread(self):
        while self._message_thread_status:
            message = self.comm.recvMessage()
            self.message_executor.handleMessage(message)
            # print(self.message_executor._finger_table._table)

    def _initMessageThread(self):
        self._message_thread_status = True
        self._message_thread = threading.Thread(target = self._messageThread)
        self._message_thread.start()
    
    def _userInputThread(self):

        while True:
            command = input(">")
            command = command.split(" ")
            if command[0] == "insert" or command[0] == "update":
                message = self.message_executor._message_creator.createPutKey(int(command[1]), command[2], self.replicate_factor)
            elif command[0] == "search":
                message = self.message_executor._message_creator.createGetKey(int(command[1]))
            elif command[0] == "delete":
                message = self.message_executor._message_creator.createDelKey(int(command[1]), self.replicate_factor)
            elif command[0] == "exit":
                self._message_thread_status = False
                break
            else:
                continue
            routeMessage = self.message_executor._message_creator.createRouteMessage(int(command[1]), message)
            self.message_executor.handleMessage(routeMessage)
            time.sleep(0.5)

    def run(self):
        self._initMessageThread()
        self._userInputThread()
        self._message_thread.join()

if __name__ == "__main__":
    try:

        port = int(sys.argv[1])
        maxnodes = int(sys.argv[2])
        node = Node(port, maxnodes)
        if len(sys.argv) == 5:
            node.initialize(sys.argv[3], int(sys.argv[4]))
        node.run()

    except Exception as e:
        raise e
