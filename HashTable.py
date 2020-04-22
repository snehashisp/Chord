import copy

class HashTable():

	def __init__(self):
		self._hash_map = {}

	def putKeyValue(self, key, value):
		self._hash_map[key] = value

	def getValue(self, key):
		return self._hash_map.get(key, None)

	def getNodeInfo(self, nodeid):
		return self._hash_map.get(nodeid, None)

	def putNodeInfo(self, nodeid, ip, port):
		self._hash_map[nodeid] = {"ip":ip, "port":port}
		self._hash_map[ip + ":" + str(port)] = nodeid

	def deleteKey(self, key):
		self._hash_map.pop(key)

	def deleteNode(self, nodeid):
		data = self._hash_map.pop(nodeid)
		self._hash_map.pop(data["ip"] + ":" + str(data["port"]))

	def getNodeId(self, ip_port):
		return self._hash_map.get(ip_port, None)

	def createFingerTableInfo(self, nodeids):
		finger_info = []
		for nodeid in nodeids:
			node_info = copy.deepcopy(self.getNodeInfo(nodeid))
			if node_info:
				node_info.update({"nodeid":nodeid})
				finger_info.append(node_info)
		return finger_info