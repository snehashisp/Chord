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

	def _execRetrieve(self, key, initiator, retrive_count):
		successor = self._hash_table.getNodeInfo(self._finger_table.getSuccessorNode())
		if successor:
			retrieve_message = self._message_creator.createRetrieve(key, initiator, retrive_count)
			self._comm.sendMessage(retrieve_message, successor["ip"], successor["port"])
		error_message = self._message_creator.createError({"key":key,"response":"NOT FOUND"})
		self._comm.sendMessage(error_message, initiator["ip"], initiator["port"])

	def _execKeyLookup(self, key, initiator):
		value = self._hash_table.getValue(key):
		if value:
			success_message = 


	def getKey(self, getKeyMessage):
		keyMessage = json.dumps(getKeyMessage)
		self._execKeyLookup(keyMessage)

	def retrieveKey(self, retrieveKeyMessage):
		keyMessage = json.dumsps(retrieveKeyMessage)

