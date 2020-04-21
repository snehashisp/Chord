class HashTable():

	def __init__(self):
		self._hash_map = {}

	def putKeyValue(self, key, value):
		self._hash_map[key] = value

	def getValue(self, key):
		return self._hash_map.get(key, None)

	def getNodeInfo(self, nodeid):
		return self._hash_map.get(key, None)

	def putNodeInfo(self, nodeid, ip, port):
		self._hash_map[nodeid] = {"ip":ip, "port":port}

	def deleteKey(self, key):
		self._hash_map.pop(key)

	def deleteNode(self, nodeid):
		self._hash_map.pop(nodeid)
