import select
import socket
import pickle
import sqlite3
import threading
import json
import os
import tkinter as tk
from datetime import datetime
import time


class Admin:
	BUFSIZE = 8192
	IP = socket.gethostbyname_ex(socket.gethostname())[-1][0]
	PORT = 8000

	def __init__(self):
		"""
		init function
		"""
		self.db = sqlite3.connect(r'segment_data.db', check_same_thread=False)
		cur = self.db.cursor()
		cur.execute("""CREATE TABLE IF NOT EXISTS segment_data(
           file_name TEXT,
           segment_index INT,
           max_index INT,
           user_hash TEXT);
        """)
		self.db.commit()
		self.current_hash_to_addr = Admin.load_dict_from_json()  # load dict from json

		self.online_peers = {}

	def search_for_parts(self, file_name: str, segment_index: int) -> list:
		"""
		query a specific filter from segments database
		:param file_name: file name
		:param segment_index: segment index
		:return: result of query for name and index
		"""
		# query for the all users that have a segment of a file
		cur = self.db.cursor()
		cur.execute("SELECT * FROM segment_data WHERE file_name = ? AND segment_index = ?", (file_name, segment_index))
		return cur.fetchall()

	def upload_to_db(self, data: dict) -> None:
		"""
		upload a file segment to database
		:param data: file segment
		:return: None
		"""
		cur = self.db.cursor()
		cur.execute("INSERT INTO segment_data VALUES(?, ?, ?, ?);",
					(data['file name'], data['index'], data['max index'], data['user hash']))
		self.db.commit()

	def get_distinct_files(self) -> list:
		"""
		filter the database for distinct file names to prevent doubles
		:return: results of query
		"""
		cur = self.db.cursor()
		# Query distinct file_name values
		cur.execute("SELECT DISTINCT file_name FROM segment_data")
		distinct_file_names = cur.fetchall()

		names = list(map(lambda x: x[0], distinct_file_names))
		return names

	@staticmethod
	def backup_dict_to_json(data, file_path='status.json') -> None:
		"""
		saves the status dict as json
		:param data: status dict
		:param file_path: path
		:return: None
		"""
		for key in data:
			data[key]['isOnline'] = False
		with open(file_path, "w") as file:
			json.dump(data, file)

	@staticmethod
	def load_dict_from_json(file_path='status.json') -> dict:
		"""
		loads a dict from the backup json file
		:param file_path: path
		:return: status dict retrieved from jsom file
		"""
		if os.path.exists(file_path):
			with open(file_path, "r") as file:
				data = json.load(file)
			return data
		else:
			return {}

	def run_admin_server(self, root: tk.Tk, listboxes: tuple) -> None:
		"""
		runs the admin server. main function.
		:param root: tkinter window object
		:param list boxes: tuple of tkinter list box objects
		:return: None
		"""
		# Create a TCP socket
		server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		# Bind the socket to a specific address and port
		server_socket.bind((Admin.IP, Admin.PORT))
		server_socket.setblocking(0)

		# Listen for incoming connections
		server_socket.listen()

		# update gui
		update_listboxes(self, root, listboxes, f"Admin server is listening on {Admin.IP} , {Admin.PORT}")

		# List to keep track of client sockets
		client_sockets = [server_socket]

		# update online online peers
		threading.Thread(target=self.get_online_peers, daemon=True, args=(root, listboxes, 10)).start()

		while True:
			# Use select to monitor sockets for read, write, and exception events
			readable, _, exceptional = select.select(client_sockets, client_sockets, client_sockets)

			for sock in readable:
				if sock == server_socket:
					# Accept new client connection
					client_socket, client_address = server_socket.accept()
					# print(f"New client connected: {client_address}")
					client_sockets.append(client_socket)
				else:
					try:
						# Receive data from client
						data = sock.recv(Admin.BUFSIZE)
						if data:
							data = pickle.loads(data)
							# print(data)
							# struct = {user hash: str, server sock: (int, str), command: str, data: {} (optional)}

							# update user current sockets
							self.current_hash_to_addr[data['user hash']] = {
								'addr': data['server sock'],
								'isOnline': True,
								'client sock': sock.getpeername(),
								'last seen': time.time()}

							# update gui
							# update_listboxes(self, root, listboxes)

							# back up status dict
							Admin.backup_dict_to_json(self.current_hash_to_addr)

							# respond to command
							if data['command'] == 'hello admin!':
								# when a peer enters the network it will update the admin on its status
								update_listboxes(self, root, listboxes, f"{data['user hash']} is saying hello!")

							# upload a reference to the database
							elif data['command'] == 'uploaded segment':
								# update gui
								update_listboxes(self, root, listboxes,
												 f"{data['user hash']} has updated database with {data['data']}")

								self.upload_to_db(data['data'])

							# return a list of online peers
							elif data['command'] == 'get online peers':
								online_peers = self.online_peers
								sendThis = ('update online peers', 0, online_peers)  # [1]: just a number
								# create temp socket to connect to the peer server
								tempSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
								tempSock.connect(data['server sock'])
								tempSock.send(pickle.dumps(sendThis))
								tempSock.close()

							# return the addr of a socket who has a segment of a file
							elif data['command'] == 'request file':
								# update gui
								update_listboxes(self, root, listboxes,
												 f"{data['user hash']} has requested addresses of {data['data']['file name']}")

								threading.Thread(target=self.sendSegmentsAddr, daemon=True,
												 args=(sock, data['data']['file name'], data['server sock'])).start()

							# return a list of all file_name in database
							elif data['command'] == 'get file names':
								# print(data)
								files = self.get_distinct_files()
								sendThis = ('distinct names', 0, files)
								tempSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
								tempSock.connect(data['server sock'])
								tempSock.send(pickle.dumps(sendThis))
								tempSock.close()

						else:
							# Client closed the connection
							sock.close()
							client_sockets.remove(sock)

					except Exception as e:
						# Client closed the connection
						sock.close()
						client_sockets.remove(sock)

			for sock in exceptional:
				# update gui
				update_listboxes(self, root, listboxes, f"Exceptional condition on socket: {sock.getpeername()}")
				sock.close()
				client_sockets.remove(sock)

	def sendSegmentsAddr(self, sock: socket.socket, file_name: str, addr: tuple) -> None:
		"""
		queries the database for suitable addresses for each one of the segments of a file
		:param sock: socket object
		:param file_name: file name
		:param addr: tuple of (ip, port)
		:return: None
		"""
		# get the max_index of the file
		cur = self.db.cursor()
		cur.execute("SELECT * FROM segment_data WHERE file_name = ?", (file_name,))
		response = cur.fetchone()

		if not response:  # file doesn't exist
			sock.send(pickle.dumps(f'File {file_name} not found on database'))
			return

		max_index = int(response[2])

		peers = []
		online_peers = self.online_peers
		for index in range(0, max_index + 1):
			cur.execute("SELECT * FROM segment_data WHERE file_name = ? AND segment_index = ?", (file_name, index))
			response = cur.fetchall()
			response = list(filter(lambda peer: peer[3] in online_peers, response))

			if not response:  # could not find a segment. the database is corrupted or every user having it is offline. same problem though
				sock.send(pickle.dumps(f'Could not retrieve {file_name} from database'))
				return

			# (file_name, index ,max_index, server_sock: (port, ip)) : tuple
			peers.append(
				(response[0][0], response[0][1], response[0][2], self.current_hash_to_addr[response[0][3]]['addr']))

		# send the list of peers
		sendThis = ('file references', 0, peers)  # [1]: just a number
		tempSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		tempSock.connect(addr)
		tempSock.send(pickle.dumps(sendThis))
		tempSock.close()

	def get_online_peers(self, root, listboxes, delay: float) -> dict:
		"""
		sets the server socket addresses of all the online peers
		:param root: window object
		:param listboxes: listboxes tuple
		:param delay: how long can a peer considered online without sending anything
		:return: a dict containing all online peers on the network right now
		"""
		while True:
			online_peers = {}
			for user_hash in self.current_hash_to_addr.keys():
				if time.time() - self.current_hash_to_addr[user_hash]['last seen'] > delay:
					self.current_hash_to_addr[user_hash]['isOnline'] = False
				else:
					self.current_hash_to_addr[user_hash]['isOnline'] = True
					online_peers[user_hash] = self.current_hash_to_addr[user_hash]['addr']  # tuple: (ip, port)
			update_listboxes(self, root, listboxes)
			self.online_peers = online_peers
			time.sleep(5)


def update_listboxes(admin: Admin, root, listboxes, text: str = None) -> None:
	"""
	updates gui
	:param admin: Admin object
	:param root: tkinter window object
	:param listboxes: list boxes objects
	:param text: text to present
	:return: None
	"""
	# delete values
	listboxes[1].delete(1, tk.END)
	# update values of right
	for user_hash, data in admin.current_hash_to_addr.items():
		listboxes[1].insert("end", user_hash + ' - Last seen: ' + "{:.2f}".format(time.time() - admin.current_hash_to_addr[user_hash]['last seen']) + ' s ago')
		if data['isOnline']:
			listboxes[1].itemconfig("end", fg="#1AFF1A")
		else:
			listboxes[1].itemconfig("end", fg="#DB0D88")
	# update values of left
	if text:
		text = str(f"({datetime.now().strftime('%H:%M:%S')}): " + text)
		listboxes[0].insert("end", text)
		listboxes[0].see(tk.END)

	root.update()


def main(admin: Admin) -> None:
	"""
	creates the gui
	:param admin: Admin instance
	:return: None
	"""
	root = tk.Tk()

	# window props
	root.minsize(600, 600)
	root.iconbitmap("torrent_icon-icons.ico")
	window_width = 1280  # get the screen dimension
	window_height = 720
	root.geometry(
		f'{window_width}x{window_height}+{int(root.winfo_screenwidth() / 2 - window_width / 2)}+{int(root.winfo_screenheight() / 2 - window_height / 2)}')

	# add title
	root.title("Torrent Management Server GUI")
	root.configure(bg="#2C205B")

	# top label
	title_label = tk.Label(root, text="Management Server GUI", fg="#A3FF75", font=('Calibri', '32', 'bold'),
						   bg="#2C205B")
	title_label.pack(pady=10)

	# left frame
	left_frame = tk.Frame(root, bg="#2C205B")
	left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

	# left listbox
	left_listbox = tk.Listbox(left_frame, bg="#33217C", font=('Arial', 16), fg="#F7F5FF")
	left_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)
	left_listbox.insert("end", "   Server Log")
	left_listbox.itemconfig("end", bg="#FF174D")

	# right frame
	right_frame = tk.Frame(root, bg="#2C205B")
	right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

	# right listbox
	right_listbox = tk.Listbox(right_frame, bg="#33217C", font=('Arial', 16), fg="#F7F5FF")
	right_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)
	right_listbox.insert("end", "   Users")
	right_listbox.itemconfig("end", bg="#FF174D")

	threading.Thread(target=admin.run_admin_server, daemon=True, args=(root, (left_listbox, right_listbox))).start()

	root.mainloop()


if __name__ == '__main__':
	# code starts here
	try:
		main(Admin())
	except:
		pass
	finally:
		quit()
