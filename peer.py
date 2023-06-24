import os
import socket
import threading
import sqlite3
import pickle
import time
from math import floor
import tkinter as tk
from tkinter import filedialog
# from tkinter import ttk
from datetime import datetime

#from admin import Admin


class Peer:
	BUFSIZE = 1024 * 256
	ADMIN_IP = '192.168.1.165' #Admin.IP
	ADMIN_PORT = 8000 #Admin.PORT

	def __init__(self, user_hash):
		"""
		init function
		:param user_hash: username
		"""
		# server side
		self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.server_sock.bind((socket.gethostname(), 0))

		self.user_hash = user_hash
		self.online_peers = {self.user_hash: (self.server_sock.getsockname()[0], self.server_sock.getsockname()[1])}

		self.names = list()

		self.gui = tuple()

		self.db = sqlite3.connect(f'{self.user_hash}.db', check_same_thread=False)
		cur = self.db.cursor()
		cur.execute(f"""CREATE TABLE IF NOT EXISTS {self.user_hash}(
		           file_name TEXT,
		           segment_index INT,
		           max_index INT,
		           data BLOB);
		        """)
		self.db.commit()

		self.data_list = list()

		self.temp = None  # general use var for global purposes
		self.ack_flag = [False, False]

		self.server_queue = list()
		self.server_online = False
		self.download_flag = False

	@staticmethod
	def divide_to_chunks(path: str, N: int) -> list:
		"""
		divide a file into N chunks of data
		:param path: path of file
		:param N: number of chunks
		:return:
		"""
		# Check if the file exists
		if os.path.isfile(path):
			# Open the file in binary mode
			with open(path, 'rb') as file:
				# Get the file size using the stat function
				size = os.stat(file.fileno()).st_size
				segments = []
				for i in range(N):
					segments.append(file.read(size // N)) if i < N - 1 else segments.append(file.read(size // N + N))

			return segments
		else:
			raise FileExistsError("File doesn't exist")

	@staticmethod
	def divide_to_peers(path: str, N: int, M: int = 0.5) -> list:
		"""
		creates a combination of segments to send to each peer in the network
		:param path: path of file
		:param N: number of connected peers
		:param M: max part of peers that can crash
		:return:
		"""
		# divide the file into chunks
		chunks = Peer.divide_to_chunks(path, N)
		factor_of_freedom = floor(N // (1 / M) + 1)

		# create the combination list of which segments to send to each peer
		parts = []
		for i in range(N):
			combination = []
			for j in range(factor_of_freedom):
				combination.append(chunks[(i + j) % N])
			parts.append(combination)

		return parts, chunks

	def upload_to_db(self, name: str, index: int, max_index: int, data: bytes) -> None:
		"""
		uploads a data segment to the database
		:param name: file name
		:param index: index of segment
		:param max_index: highest segment number of og file
		:param data: raw data
		:return: None
		"""
		cur = self.db.cursor()
		cur.execute(f"INSERT INTO {self.user_hash} VALUES(?, ?, ?, ?);",
					(name, index, max_index, data))
		self.db.commit()

	def search_for_parts(self, file_name: str, segment_index: int) -> list:
		"""
		queries for segments that match the file name and index
		:param file_name: file name
		:param segment_index: index of segment
		:return: a list of all matches for the query
		"""
		# query for the all users that have a segment of a file
		cur = self.db.cursor()
		cur.execute(f"SELECT * FROM {self.user_hash} WHERE file_name = ? AND segment_index = ? AND data IS NOT NULL",
					(file_name, segment_index))

		return cur.fetchone()

	def user_upload(self, delay: float = 0.1) -> None:
		"""
		connects between the upload procedure (file_upload) and the gui
		:param delay: some time between sending packages
		:return: None
		"""
		path = filedialog.askopenfilename()
		if not path:
			return
		file_name = path.split('/')[-1]

		# check compatibility

		# say hello to admin
		client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sendThis = {'user hash': self.user_hash,
					'server sock': (self.server_sock.getsockname()[0], self.server_sock.getsockname()[1]),
					'command': 'hello admin!'}
		try:
			client_sock.connect((Peer.ADMIN_IP, Peer.ADMIN_PORT,))
			self.server_online = True
			client_sock.sendall(pickle.dumps(sendThis))
			time.sleep(delay)
		except:
			self.server_online = False
			self.server_queue.append(sendThis)

		# get file names
		item = tk.Listbox(self.gui[0], selectmode=tk.SINGLE).get(tk.ACTIVE)
		items = self.gui[2].get(0, tk.END)
		if item in items:
			self.update_listboxes('You cannot upload a file with a name that already exists!')
			return

		# compatibility approved, upload the file
		self.file_upload(client_sock, path)

		client_sock.close()

		# update gui
		# self.names.append(file_name)

	def user_download(self, selected_file: str, delay: float = 0.1) -> None:
		"""
		connects between the download procedure (file_download) and the gui
		:param selected_file: a file that is present in the database
		:param delay: some time to wait between packages
		:return: None
		"""
		dir = filedialog.askdirectory()
		if not dir:
			return

		# say hello to admin
		sendThis = {'user hash': self.user_hash,
					'server sock': (self.server_sock.getsockname()[0], self.server_sock.getsockname()[1]),
					'command': 'hello admin!'}
		try:
			client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			client_sock.connect((Peer.ADMIN_IP, Peer.ADMIN_PORT,))
			self.server_online = True

			client_sock.sendall(pickle.dumps(sendThis))
			time.sleep(delay)

			# download the file into dir
			self.file_download(client_sock, selected_file, dir)

			client_sock.close()

		except:
			self.server_queue.append(sendThis)
			self.server_online = False
			self.update_listboxes('It is not possible to download a file without the management server online!')

	def file_upload(self, client_sock: socket.socket, path: str, delay: float = 0.1) -> None:
		"""
		the upload procedure. communicates with the admin server and other peers to upload the file to the network
		:param client_sock: the client sock of the peer
		:param path: path of desired file
		:param delay: some time to wait between packages
		:return: None
		"""
		try:
			# upload procedure
			file_name = path.split('/')[-1]

			# split the file into N chunks and create combinations of the file
			combination, chunks = Peer.divide_to_peers(path, len(self.online_peers.values()))
			# print(self.server_online, self.online_peers)
			# when having the list of online peers, begin to transmit the data one at the time
			for peer, designated_segments in zip(self.online_peers.values(), combination):
				tempSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				peer = tuple(peer)
				try:
					tempSock.connect(peer)
				except Exception as e:
					raise e
					self.update_listboxes('Upload failed. Try again.')
					return
				# print(self.server_online)
				for segment in designated_segments:
					# signal server first
					time.sleep(delay)
					# print(('download', file_name, chunks.index(segment), len(self.online_peers) - 1))
					tempSock.sendall(
						pickle.dumps(('download', file_name, chunks.index(segment), len(self.online_peers) - 1)))

					# start sending data
					data = [segment[j:j + Peer.BUFSIZE - 100] for j in range(0, len(segment), Peer.BUFSIZE - 100)]
					for index, c in enumerate(data):
						time.sleep(delay)
						# print(index, len(data))
						tempSock.sendall(pickle.dumps((c, index != len(data) - 1)))  # update more flag

				# close connection before opening a new one
				tempSock.close()
		except (OSError, ConnectionError, ConnectionResetError, Exception) as e:
			raise e

	def file_download(self, client_sock: socket.socket, file_name: str, dir: str, delay: float = 0.07) -> None:
		"""
		the download procedure. communicates with the admin server and other peers to download the file from the network
		:param client_sock: the client sock of the peer
		:param file_name: file name
		:param dir: where to save the file once obtained
		:param delay: some time to wait between packages
		:return: None
		"""
		try:
			# download procedure
			time.sleep(delay)

			'''
			# get file names
			self.names = [0]
			sendThis = {'user hash': self.user_hash,
						'server sock': (self.server_sock.getsockname()[0], self.server_sock.getsockname()[1]),
						'command': 'get file names'}
			client_sock.sendall(pickle.dumps(sendThis))
			while self.names == [0]:  # wait until received
				pass

			if file_name not in self.names:
				self.update_listboxes('File is not in database. Try another one.')
				return
			'''

			# if exists, request the references of the file
			self.temp = [0]
			sendThis = {'user hash': self.user_hash,
						'server sock': (self.server_sock.getsockname()[0], self.server_sock.getsockname()[1]),
						'command': 'request file',
						'data':
							{'file name': file_name}
						}
			client_sock.sendall(pickle.dumps(sendThis))

			i = 0
			while self.temp == [0]:  # wait until received
				i += 1
				if i > 10000000:
					raise ConnectionError
			self.data_list = [None] * len(self.temp)
			self.download_flag = False
			for i, reference in enumerate(self.temp):
				# print(reference)

				file_name, index, max_index, addr = reference  # (file name, index, max index, addr)

				# check for a missing index, which means the admin database is corrupted or everyone is offline
				if i != index:
					self.update_listboxes(
						'The database did not contain a suitable address of a segment and therefore it is unreachable. Download failed.')
					return
				tempSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				addr = tuple(addr)
				try:  # if socket is offline or something else happened
					tempSock.connect(addr)
				except Exception as e:
					self.update_listboxes('Download failed. There is no peer with the data.')
					return
				time.sleep(delay)
				tempSock.sendall(
					pickle.dumps(('upload to', (self.server_sock.getsockname()[0], self.server_sock.getsockname()[1]),
								  file_name, index, max_index)))

				# close connection
				tempSock.close()

			while not self.download_flag:
				# print(list(map(lambda x: 0 if x is not None else None, self.data_list))) # debug
				pass
			self.download_flag = False
			# arrange and merge
			file_data = b''
			for n in self.data_list:
				# print(len(n)) # debug
				file_data += n
			# save file
			path = str(dir + '/' + file_name)
			with open(path, "wb") as file:
				file.write(file_data)

			self.update_listboxes(f'Download was successful. File saved at {path}')

		except Exception as e:
			# print(e)
			self.update_listboxes('Download failed. Could not retrieve the file.')
			return

	def handle_client(self, conn: socket.socket, addr: tuple, delay: float = 0.1) -> None:
		"""
		a thread to handle one connection as part of the peer server side
		:param delay: delay time
		:param conn: socket object
		:param addr: address (ip, port)
		:return: None
		"""
		download_blocking = False
		upload_blocking = False
		name, index, max_index, file_data = None, None, None, None

		#try:
		while True:
			data = conn.recv(Peer.BUFSIZE)

			if data:
				# handle data
				# print(data)
				data = pickle.loads(data)
				# print(data)

				if download_blocking:  # in the middle of file download transmission
					# print(data[1])
					if not data[1]:  # no more segments
						file_data += data[0]
						download_blocking = False
						# print(len(file_data))
						self.upload_to_db(name, index, max_index, file_data)
						self.update_listboxes(f'Received a data segment: {(name, index, max_index)}')

						# update admin server
						sendThis = {'user hash': self.user_hash,
									'server sock': (
									self.server_sock.getsockname()[0], self.server_sock.getsockname()[1]),
									'command': 'uploaded segment',
									'data':
										{'file name': name,
										 'index': index,
										 'max index': max_index,
										 'user hash': self.user_hash}
									}
						if self.server_online:
							time.sleep(delay)
							tempSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
							tempSock.connect((Peer.ADMIN_IP, Peer.ADMIN_PORT,))
							tempSock.sendall(pickle.dumps(sendThis))
							tempSock.close()
							time.sleep(delay)
						else:
							self.server_queue.append(sendThis)

					else:  # add to data and wait for more
						file_data += data[0]
					continue

				if upload_blocking:
					# print(data[1])
					if not data[1]:  # no more segments
						file_data += data[0]
						self.data_list[index] = file_data
						upload_blocking = False
						self.download_flag = True
						self.update_listboxes(f'Received a data segment: {(name, index, max_index)}')

					else:  # add to data and wait for more
						file_data += data[0]
					continue

				# struct = (command: str, server sock: (ip, port), data: {} or []) : tuple
				if data[0] == 'download':  # preparing for downloading something
					name, index, max_index = data[1:]
					file_data = b''
					download_blocking = True

				elif data[0] == 'prepare for data requested':  # preparing to upload something to someone
					name, index, max_index = data[1:]
					file_data = b''
					upload_blocking = True
					self.download_flag = False

				elif data[0] == 'upload to':  # start a thread to upload a segment to someone
					# upload the file from db
					file = self.search_for_parts(data[2], data[3])
					name, index, max_index = data[2:]
					# send the data to the specified address
					threading.Thread(target=self.send_segment, args=(file, data[1],)).start()

				elif data[0] == 'update online peers':
					self.online_peers = data[2]

				elif data[0] == 'distinct names':
					self.names = data[2]

				elif data[0] == 'file references':
					self.temp = data[2]

		'''except Exception as e:
			print(data)
			raise e
			conn.close()
			return'''

	def send_segment(self, file: tuple, addr: tuple, delay: float = 0.07, iteration: bool = False) -> None:
		"""
		often the segments will be larger that the buffer size so this function chops it to chunks and sends every one of them
		:param iteration: is second iteration?
		:param file: file name
		:param addr: address of peers server socket (ip, port)
		:param delay: some time to wait between packages
		:return: None
		"""
		tempSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		addr = tuple(addr)
		try:
			file_data = bytearray(file[3])
		except:
			# tempSock.send(pickle.dumps('Could not retrieve the file from database'))
			return

		tempSock.connect(addr)
		# notify server
		tempSock.sendall(pickle.dumps(('prepare for data requested', file[0], file[1], file[2])))
		time.sleep(delay)

		# start sending data
		data = [file_data[j:j + Peer.BUFSIZE - 100] for j in range(0, len(file_data), Peer.BUFSIZE - 100)]
		for i, chunk in enumerate(data):
			time.sleep(delay)
			# tempSock.sendall(pickle.dumps((str(str(file[4]) + str(i)), chunk)))  # debug
			# print(i, len(data))
			tempSock.sendall(pickle.dumps((chunk, i != len(data) - 1)))  # update more flag

		time.sleep(delay)
		time.sleep(delay)

		tempSock.close()

	def run_server(self) -> None:
		"""
		the main loop of the peer's server side
		:return: None
		"""
		while True:
			try:
				# bind server
				self.update_listboxes(
					f"server bound at ({self.server_sock.getsockname()[0]} , {self.server_sock.getsockname()[1]})")

				while True:
					# listen for an incoming connection
					self.server_sock.listen()
					# accept incoming connection
					conn, addr = self.server_sock.accept()
					# self.update_listboxes(f'New connection from {addr}') # debug

					threading.Thread(target=self.handle_client, daemon=True, args=(conn, addr,)).start()

			except Exception as e:
				continue

	def update_listboxes(self, text: str) -> None:
		"""
		updates the log list box
		:param text: text to add to the list box
		:return: None
		"""
		# update values of left
		text = str(f"({datetime.now().strftime('%H:%M:%S')}): " + text)
		self.gui[1].insert("end", text)
		self.gui[1].see(tk.END)
		self.gui[0].update()


def main(peer: Peer) -> None:
	"""
	creates the gui
	:param peer: Peer instance
	:return: None
	"""
	# window props
	root = tk.Tk()
	root.minsize(600, 600)
	window_width = 1024  # 1280  # get the screen dimension
	window_height = 720
	root.geometry(
		f'{window_width}x{window_height}+{int(root.winfo_screenwidth() / 2 - window_width / 2)}+{int(root.winfo_screenheight() / 2 - window_height / 2)}')

	# add title
	root.title("Torrent Peer GUI")
	root.configure(bg="#2C205B")
	root.iconbitmap("torrent_icon-icons.ico")

	# add top label
	title_label = tk.Label(root, text="Peer GUI", fg="#A3FF75", font=('Calibri', '32', 'bold'), bg="#33217C")
	title_label.grid(row=0, column=0, rowspan=1, columnspan=11, padx=0, pady=0, sticky="news")

	# create left listbox
	left_listbox = tk.Listbox(root, bg="#33217C", font=('Arial', 16), fg="#F7F5FF")
	left_listbox.grid(row=1, column=0, rowspan=4, columnspan=5, padx=10, pady=10, sticky="news")
	left_listbox.insert("end", "   Log")
	left_listbox.itemconfig("end", bg="#FF174D")

	# create right listbox
	right_listbox = tk.Listbox(root, bg="#33217C", font=('Arial', 16), fg="#F7F5FF")
	right_listbox.grid(row=1, column=5, rowspan=4, columnspan=4, padx=10, pady=10, sticky="news")
	right_listbox.insert("end", "   Select file to download")
	right_listbox.itemconfig("end", bg="#FF174D")

	# create left button
	left_button = tk.Button(root, text="Upload", bg="#6340F3", font=('Calibri', 18, 'bold'),
							fg="#F9F9F9",
							command=lambda: threading.Thread(target=peer.user_upload, daemon=True).start())
	left_button.grid(row=6, column=2, rowspan=1, columnspan=1, padx=10, pady=10, sticky="news")

	# create right button
	right_button = tk.Button(root, text="Download", bg="#6340F3", font=('Calibri', 18, 'bold'),
							 fg="#F9F9F9", command=lambda: threading.Thread(target=handle_button_click,
																			args=(peer, right_listbox)).start())
	right_button.grid(row=6, column=5, rowspan=1, columnspan=1, padx=10, pady=10, sticky="news")

	# make resizes look good
	for i in range(6):
		root.columnconfigure(i, weight=1)
	for i in range(11):
		root.rowconfigure(i, weight=1)

	peer.gui = (root, left_listbox, right_listbox)

	threading.Thread(target=peer.run_server, daemon=True).start()
	threading.Thread(target=get_names, args=(peer, root, right_listbox)).start()

	root.mainloop()


def get_names(peer: Peer, root: tk.Tk, right_listbox) -> None:
	"""
	keeps a live communication with the admin server to spot file uploads and change the right list box
	:param peer: Peer instance
	:param root: window object
	:param right_listbox: listbox object
	:return: None
	"""
	try:
		noted = False
		while True:
			# get files names
			try:
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((Peer.ADMIN_IP, Peer.ADMIN_PORT,))
				peer.server_online = True
				noted = False

				# update the admin with what happened while it was offline
				time.sleep(0.1)
				if peer.server_queue:
					sock.sendall(pickle.dumps(peer.server_queue.pop(0)))

				sendThis = {'user hash': peer.user_hash,
							'server sock': (peer.server_sock.getsockname()[0], peer.server_sock.getsockname()[1]),
							'command': 'get file names'}
				sock.sendall(pickle.dumps(sendThis))
				time.sleep(0.1)
				sendThis = {'user hash': peer.user_hash,
							'server sock': (peer.server_sock.getsockname()[0], peer.server_sock.getsockname()[1]),
							'command': 'get online peers'}
				sock.sendall(pickle.dumps(sendThis))

			except (ConnectionError, ConnectionResetError, OSError) as e:
				peer.server_online = False
				if not noted:
					peer.update_listboxes('The connection with admin server was terminated unexpectedly.')
					noted = True

			finally:
				items = [right_listbox.get(index) for index in range(right_listbox.size())]
				if list(filter(lambda x: x not in items, peer.names)):
					root.after(500, lambda: right_listbox.delete(1, tk.END))
					root.after(500, lambda: [right_listbox.insert("end", name) for name in peer.names])
					root.update()
				time.sleep(2)



	except (RuntimeError) as e:
		time.sleep(2)
		root.destroy()
		return


def handle_button_click(peer: Peer, listbox) -> None:
	"""
	gets the item selected by the curser before calling to download it
	:param peer: Peer instance
	:param listbox: listbox object
	:return: None
	"""
	try:
		selected_item = listbox.get(listbox.curselection())
	except:
		return
	# can't select first item
	if selected_item == "   Select file to download":
		return
	peer.user_download(selected_item)


if __name__ == '__main__':
	try:
		# retrieve user name if logged in before
		filename = 'username.txt'
		if not os.path.exists(filename):
			hex_hash = os.urandom(4).hex()
			text = 'USER' + hex_hash
			with open(filename, 'w') as file:
				file.write(text)
		with open(filename, 'r') as file:
			text = file.read()

		# start gui
		main(Peer(text))
	except:
		pass
	finally:
		quit()
