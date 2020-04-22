import socket

class Comm():

	def __init__(self, port):
		self.hostname = socket.gethostname()
		self.ip = socket.gethostbyname(self.hostname)
		self.port = port
		self.receive_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.receive_socket.bind((self.ip, self.port))

	def getIpPort(self):
		return self.ip + ":" + str(self.port)

	#send a message to a chord node, message must be string
	def sendMessage(self, message: str, ip: str, port: int):
		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sock.sendto(message.encode(), (ip, port))
		except:
			raise Exception("conn_error," + ip + ":" + str(port))

	def recvMessage(self):
		data, addr = self.receive_socket.recvfrom(4096)
		return data.decode()
