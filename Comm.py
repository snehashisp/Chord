import socket

class Comm():

	def __init__(self, port):
		self.hostname = socket.gethostname()
		self.ip = socket.gethostbyname(self.hostname)
		self.port = port
		self.receive_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.receive_socket.bind((self.ip, self.port))

	#send a message to a chord node, message must be string
	def sendMessage(self, ip: str, port: int, message: str):
		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sock.sendto(message.encode(), (ip, port))
		except:
			raise Exception(ip + ":" + str(port))

	def recvMessage(self):
		return self.receive_socket.recvfrom(4096).decode()
