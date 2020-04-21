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
				self._message_creator.createError({"key":key,"response":"NOT FOUND"}),
				initiator["ip"], 
				initiator["port"]
			)

	def _execKeyLookup(self, key, initiator, updator = None):
		value = self._hash_table.getValue(key):
		if value:
			self._comm.sendMessage(
				self._message_creator.createResponse({"key":key, "value":value})
				initiator["ip"],
				initiator["port"]
			)
			if updator:
				self._comm.sendMessage(
					self._message_creator.createPutKey(key, value, 1)
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

	def putKey(self, putKeyMessage):
		keyMessage = json.loads(putKeyMessage)
		self._hash_table.putKeyValue(keyMessage["info"]["key"], keyMessage["info"]["value"])
		keyMessage["replicate"] -= 1
		if keyMessage["replicate"] > 0:
			next_node = self._getNextNode()
			if next_node:
				self._comm.sendMessage(
					json.dumps(keyMessage),
					next_node["ip"],
					next_node["port"]
				)

	def routeKeyMessage(self, routeMessage):
		routeMessage = json.loads(routeMessage)
		next_node = self._finger_table.getNextHop(keyMessage["info"])
		successor = self._finger_table.getSuccessorNode()
		next_node = self._hash_table.getNodeInfo(next_node)
		if next_node == successor:
			message = json.dumps(routeMessage["message"])
		else:
			message = json.dumps(routeMessage)
		self._comm.sendMessage(message, next_node["ip"], next_node["port"])

	



