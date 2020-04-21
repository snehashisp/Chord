import json

class Messages():

	def __init__(self, ip, port, nodeid):
		self._message = {
			"meta": {
				"ip": ip,
				"port": port,
				"nodeid": nodeid
			}
		}

	#new node join message
	def createJoin(self):
		return json.dumps(self._message.update({
			"type":"join"
			}))

	#retrieve value from key message
	def createGetKey(self, key):
		return json.dumps(self._message.update({
			"type":"get",
			"info":key
			}))

	#route a message to its destination predecessor
	def createRouteMessage(self, key, message):
		return json.dumps({
			"type":"route",
			"info":key,
			"message":message
			})

	#retrieve a key from another node to store in this node
	def createRetrieve(self, key, initiator, hoplimit):
		return json.dumps(self._message.update({
			"type":"retreive",
			"hops":hoplimit,
			"info":key,
			"initiator":initiator
			}))

	#put key value pair
	def createPutKey(self, key, value, replica_count):
		return json.dumps(self._message.update({
			"type":"put",
			"replicas":replica_count,
			"info": {
				"key":key,
				"value":value
				}
			}))
		
	#delete a key
	def createDelKey(self, key, replica_count):
		return json.dumps(self._message.update({
			"type":"del",
			"replicas": replica_count,
			"info":key
			}))

	#response to join giving predecession node information
	def createJoinResponse(self, successor_table):
		return json.dumps(self._message.update({
			"type":"join_response",
			"info": successor_table
			}))

	#update message to update finger table
	def createUpdateTable(self, nodeid):
		return json.dumps(self._message.update({
			"type":"table_update",
			"info":nodeid
			}))

	#respond to the sender on an update message
	def createUpdateTableResponse(self):
		return json.dumps(self._message.update({
			"type":"table_response"
			}))

	def createResponse(self, message):
		return json.dumps(self._message.update({
			"type":"response",
			"info":message
			}))

	def createError(self, errorMessage):
		return json.dumps(self._message.update({
			"type":"error",
			"info":errorMessage
			}))

