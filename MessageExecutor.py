from Comm import *
from Messages import *
from HashTable import *
from FingerTable import *
import json
import hashlib

class MessageExecutor():

	def __init__(self, port, maxnodes, replicate_factor = 0):
		self._comm = Comm(port)
		self._nodeid = int(hashlib.sha1(self._comm.getIpPort().encode()).hexdigest(), 16) % maxnodes
		self._message_creator = Messages(self._comm.ip, port, self._nodeid)
		self._finger_table = FingerTable(self._nodeid, maxnodes)
		self._hash_table = HashTable()
		self._replicate_factor = replicate_factor
		self._message_handler = {
			"get": self.getKey,
			"retrieve": self.retrieveKey,
			"put": self.putKey,
			"del": self.delKey,
			"join": self.join,
			"join_response": self.joinResponse,
			"table_update": self.updateTable,
			"table_response": self.updateTableResponse,
			"route": self.routeMessage,
			"error": self._infoPrinter,
			"response": self._infoPrinter
		}
		self._hash_table.putNodeInfo(self._nodeid, self._comm.ip, self._comm.port)

	def _infoPrinter(self, message):
		print(message["info"])

	def _getNextNode(self):
		successor_nodeid = self._finger_table.getSuccessorNode()
		successor = self._hash_table.getNodeInfo(successor_nodeid)
		if successor_nodeid != self._nodeid and successor:
			return successor
		return None

	def _execRetrieve(self, retrieve_message):
		next_node = self._getNextNode()
		if next_node:
			self._comm.sendMessage(
				retrieve_message,
				next_node["ip"], 
				next_node["port"]
			)
		else:
			self._comm.sendMessage(
				self._message_creator.createError({"key":retrieve_message["info"], "response":"NOT FOUND"}),
				retrieve_message["initiator"]["ip"], 
				retrieve_message["initiator"]["port"]
			)

	def _execKeyLookup(self, key, initiator, updator = None):
		value = self._hash_table.getValue(key)
		if value:
			self._comm.sendMessage(
				self._message_creator.createResponse({"key":key, "value":value}),
				initiator["ip"],
				initiator["port"]
			)
			if updator:
				self._comm.sendMessage(
					self._message_creator.createPutKey(key, value, self._replicate_factor),
					updator["ip"],
					updator["port"]
				)
			return True
		return False

	def getKey(self, keyMessage):
		#keyMessage = json.loads(getKeyMessage)
		if not self._execKeyLookup(keyMessage["info"], keyMessage["meta"]):
			if self._replicate_factor > 0:
				self._execRetrieve(self._message_creator.createRetrieve(keyMessage["info"], keyMessage["meta"], self._replicate_factor))
			else:
				self._comm.sendMessage(
					self._message_creator.createError({"key":keyMessage["info"], "response":"NOT FOUND"}),
					keyMessage["meta"]["ip"], 
					keyMessage["meta"]["port"]
				)

	def retrieveKey(self, keyMessage):
		# keyMessage = json.loads(retrieveKeyMessage)
		if not self._execKeyLookup(keyMessage["info"], keyMessage["initiator"], keyMessage["meta"]):
			keyMessage["hops"] -= 1
			if keyMessage["hops"] > 0:
				self._execRetrieve(json.dumps(keyMessage))
			else:
				self._comm.sendMessage(
					self._message_creator.createError({"key":keyMessage["info"],"response":"NOT FOUND"}),
					keyMessage["initiator"]["ip"], 
					keyMessage["initiator"]["port"]
				)

	def _execReplicate(self, replicate_message):
		replicate_message["replicas"] -= 1
		if replicate_message["replicas"] > 0:
			next_node = self._getNextNode()
			if next_node:
				self._comm.sendMessage(
					json.dumps(replicate_message),
					next_node["ip"],
					next_node["port"]
				)

	def putKey(self, keyMessage):
		# keyMessage = json.loads(putKeyMessage)
		self._hash_table.putKeyValue(keyMessage["info"]["key"], keyMessage["info"]["value"])
		self._execReplicate(keyMessage)

	def delKey(self, keyMessage):
		# keyMessage = json.loads(delKeyMessage)
		self._hash_table.deleteKey(keyMessage["info"])
		self._execReplicate(keyMessage)

	def _updateNodeInfo(self, node_info):
		self._finger_table.insertNode(node_info["nodeid"])
		self._hash_table.putNodeInfo(node_info["nodeid"], node_info["ip"], node_info["port"])

	def join(self, joinMessage):
		# joinMessage = json.loads(joinMessage)
		self._comm.sendMessage(
			self._message_creator.createJoinResponse(
				self._hash_table.createFingerTableInfo(self._finger_table.getTable())),
			joinMessage["meta"]["ip"],
			joinMessage["meta"]["port"]
		)

	def joinResponse(self, joinResponseMessage):
		# joinResponseMessage = json.loads(joinResponseMessage)
		self._updateNodeInfo(joinResponseMessage["meta"])
		for node_info in joinResponseMessage["info"]:
			self._finger_table.insertNode(node_info["nodeid"])
			self._hash_table.putNodeInfo(node_info["nodeid"], node_info["ip"], node_info["port"])


	def updateTable(self, updateTableMessage):
		# updateTableMessage = json.loads(updateTableMessage)
		self._updateNodeInfo(updateTableMessage["meta"])
		self._comm.sendMessage({
			self._message_creator.createUpdateTableResponse(),
			updateTableMessage["meta"]["ip"],
			updateTableMessage["meta"]["port"]
		})

	def updateTableResponse(self, updateTableResponseMessage):
		# updateTableResponseMessage = json.loads(updateTableResponseMessage)
		self._updateNodeInfo(updateTableResponseMessage["meta"])

	def routeMessage(self, routeMessage):
		# routeMessage = json.loads(routeMessage)
		key_successor, Key_predecessor = self._finger_table.getNextHop(routeMessage["info"])
		successor = self._finger_table.getSuccessorNode()
		# print(key_successor, Key_predecessor, successor)
		if key_successor == successor:
			message = routeMessage["message"]
			if message["type"] == "join":
				self._updateNodeInfo(message["meta"])
			message = json.dumps(message)
			next_node = key_successor
		else:
			message = json.dumps(routeMessage)
			next_node = Key_predecessor
		next_node = self._hash_table.getNodeInfo(next_node)
		self._comm.sendMessage(message, next_node["ip"], next_node["port"])
	
	def handleMessage(self, message):
		message = json.loads(message)
		print("Message Received at handler", message)
		while True:
			try:
				self._message_handler.get(message["type"])(message)
				print("Message Handled")
				break
			except Exception as e:
				# raise e
				# break
				etype, data = str(e).split(",", 1)
				if etype == 'conn_error':
					del_nodeid = self._hash_table.getNodeId(data)
					if del_nodeid:
						self._finger_table.deleteNode(del_nodeid)
						self._hash_table.deleteNode(del_nodeid)
				else:
					raise e






