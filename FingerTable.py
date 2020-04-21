import math

class FingerTable():

	def __init__(self, nodeid, maxnodes):
		self._nodeid = nodeid
		self._table = [nodeid for _ in range(math.floor(math.log2(maxnodes)))]
		self._maxnodes = maxnodes

	def getNextHop(self, nodeid):

		_index = 0
		_prev, _next = self._nodeid, self._table[_index]
		while not (nodeid >=  _prev and nodeid <= _next):
			_prev = _next
			_index += 1
			if _index == len(self._table):
				return _prev
			else:
				_next = self._table[_index]
		return _prev

	def insertNode(self, nodeid):

		_index = 0
		while _index < len(self._table):
			_next_node = (self._nodeid + 2**_index) % self._maxnodes
			if (nodeid - _next_nonde) % self._maxnodes < (self._table[_index] - _next_node) % self._maxnodes:
				self._table[_index] = nodeid
			_index += 1

	def deleteNode(self, nodeid):

		_next_node = self._nodeid
		_index = len(self._table) - 1
		while _index >= 0:
			if self._table[_index] == nodeid:
				self._table[_index] = _next_node
			else:
				_next_node = self._table[_index]
			_index -= 1

	def getSuccessorNode(self):
		return self._table[0]

