from Comm import *
from Messages import *
from HashTable import *
from FingerTable import *
import json

class MessageExecutor():

	def __init__(self, nodeid, port, maxnodes, replicate_factor = 2):
		self._comm = Comm(port)
		self._message_creator = Messages(self._comm.ip, port, nodeid)
		self._nodeid = nodeid
		self._finger_table = FingerTable(nodeid, maxnodes)
		self._hash_table = HashTable()
		self._replicate_factor = replicate_factor

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
					self._message_creator.createPutKey(key, value, 2),
					updator["ip"],
					updator["port"]
				)
			return True
		return False

	def getKey(self, getKeyMessage):
		keyMessage = json.loads(getKeyMessage)
		if not self._execKeyLookup(keyMessage["info"], keyMessage["meta"]):
			self._execRetrieve(self._message_creator.createRetrieve(keyMessage["info"], keyMessage["meta"], self._replicate_factor))

	def retrieveKey(self, retrieveKeyMessage):
		keyMessage = json.loads(retrieveKeyMessage)
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
		replicate_message["replica"] -= 1
		if replicate_message["replica"] > 0:
			next_node = self._getNextNode()
			if next_node:
				self._comm.sendMessage(
					json.dumps(replicate_message),
					next_node["ip"],
					next_node["port"]
				)

	def putKey(self, putKeyMessage):
		keyMessage = json.loads(putKeyMessage)
		self._hash_table.putKeyValue(keyMessage["info"]["key"], keyMessage["info"]["value"])
		self._execReplicate(keyMessage)

	def delKey(self, delKeyMessage):
		keyMessage = json.loads(delKeyMessage)
		self._hash_table.deleteKey(keyMessage["info"])
		self._execReplicate(keyMessage)

	def _updateNodeInfo(self, node_info):
		self._finger_table.insertNode(node_info["nodeid"])
		self._hash_table.putNodeInfo(node_info["nodeid"], node_info["ip"], node_info["port"])

	def join(self, joinMessage):
		joinMessage = json.loads(joinMessage)
		self._comm.sendMessage({
			self._message_creator.createJoinResponse(self._finger_table.getTable()),
			joinMessage["meta"]["ip"],
			joinMessage["meta"]["port"]
		})

	def joinResponse(self, joinResponseMessage):
		joinResponseMessage = json.loads(joinResponseMessage)
		self._updateNodeInfo(joinResponseMessage["meta"])
		for nodeid in joinResponseMessage["info"]:
			self._finger_table.insertNode(nodeid)

	def updateTable(self, updateTableMessage):
		updateTableMessage = json.loads(updateTableMessage)
		self._comm.sendMessage({
			self._message_creator.createUpdateTableResponse(),
			updateTableMessage["meta"]["ip"],
			updateTableMessage["meta"]["port"]
		})

	def updateTableResponse(self, updateTableResponseMessage):
		updateTableResponseMessage = json.loads(updateTableResponseMessage)
		self._updateNodeInfo(updateTableResponseMessage["meta"])

	def routeKeyMessage(self, routeMessage):
		routeMessage = json.loads(routeMessage)
		next_node = self._finger_table.getNextHop(routeMessage["info"])
		successor = self._finger_table.getSuccessorNode()
		next_node = self._hash_table.getNodeInfo(next_node)
		if next_node == successor:
			message = routeMessage["message"]
			if message["type"] == "join":
				self._updateNodeInfo(message["meta"])

		else:
			message = json.dumps(routeMessage)
		self._comm.sendMessage(message, next_node["ip"], next_node["port"])




