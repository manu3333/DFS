###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	Copy client for the DFS
#
#

import socket
import sys
import os.path

from Packet import *

def usage():
	print """Usage:\n\tFrom DFS: python %s <server>:<port>:<dfs file path> <destination file>\n\tTo   DFS: python %s <source file> <server>:<port>:<dfs file path>""" % (sys.argv[0], sys.argv[0])
	sys.exit(0)

def copyToDFS(address, fname, path):
	""" Contact the metadata server to ask to copu file fname,
	    get a list of data nodes. Open the file in path to read,
	    divide in blocks and send to the data nodes. 
	"""

	# Create a connection to the data server

	# Fill code
	sockt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sockt.connect(address) 
	
	file_size = os.path.getsize(path)
	pack = Packet()
	pack.BuildPutPacket(fname, file_size, 0) # THIS IS DUMMY BLOCK SIZE. Metadata won't need it. 
	sockt.sendall(pack.getEncodedPacket())

	status = sockt.recv(3)
	print(status)

	dat = sockt.recv(4096)
	data = Packet() 
	data.DecodePacket(dat)
	message = data.getDataNodes()
	# Read file
	read = open(path,'r+b')
	block_ids = []
	# Fill code
	if status == "NAK":
		print ("Error copying files.")
	else:
		bk_size = (file_size / len(message)) + 1
		
		for i in message:
			# sock_to_dnode.sendto
			# print(read.name, tuple(message[i]))
			sock_to_dnode = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			address1 = i[0]
			port1 = i[1]
			sock_to_dnode.connect((address1, port1))
			dnode_pack = Packet()
			dnode_pack.BuildPutPacket(fname, file_size, bk_size)
			sock_to_dnode.sendall(dnode_pack.getEncodedPacket())
			print("Put Packet sent.")

			if sock_to_dnode.recv(2) == "OK":
				sock_to_dnode.sendall(read.read(bk_size))
				block_ids.append((address1, port1, sock_to_dnode.recv(1024)))

			sock_to_dnode.close()

	sockt.close()
	read.close()





	# Create a Put packet with the fname and the length of the data,
	# and sends it to the metadata server 

	# Fill code

	# If no error or file exists
	# Get the list of data nodes.
	# Divide the file in blocks
	# Send the blocks to the data servers

	# Fill code	

	# Notify the metadata server where the blocks are saved.
	socket_blks = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	socket_blks.connect(address)

	bk_ids_to_meta = Packet()
	bk_ids_to_meta.BuildDataBlockPacket(fname, block_ids)
	socket_blks.sendall(bk_ids_to_meta.getEncodedPacket())
	socket_blks.close()
	
def copyFromDFS(address, fname, path):
	""" Contact the metadata server to ask for the file blocks of
	    the file fname.  Get the data blocks from the data nodes.
	    Saves the data in path.
	"""

   	# Contact the metadata server to ask for information of fname

   	socke = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	socke.connect(address) 

	packt = Packet()
	packt.BuildGetPacket(fname)
	socke.sendall(packt.getEncodedPacket())

	# Fill code
	md_response = socke.recv(4096)

	if md_response == "NFOUND":
		print("NO, DALE REWIND!")
	else:

		# print "tamo aki"
		# print md_response
		packt.DecodePacket(md_response)
		dnodes = packt.getDataNodes()
		# print(dnodes)
		
		file_to_append = open(path,'ab')

		blocks_string = ""

		for i in dnodes:
			print ("address:",i[0])
			print ("port:",i[1])
			print ("blockid:",i[-1])

			address1 = i[0]
			port1 = i[1]
			block_id = i[-1]
			
			sok = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sok.connect((address1, port1))

			packert = Packet()
			packert.BuildGetDataBlockPacket(block_id)
			sok.sendall(packert.getEncodedPacket())

			block = sok.recv(4096)
			print (block)
			blocks_string += block

		file_to_append.write(blocks_string)
		file_to_append.close()



	# If there is no error response Retreive the data blocks

	# Fill code
	#for i 

    	# Save the file
	
	# Fill code

if __name__ == "__main__":
#	client("localhost", 8000)
	if len(sys.argv) < 3:
		usage()

	file_from = sys.argv[1].split(":")
	file_to = sys.argv[2].split(":")

	if len(file_from) > 1:
		ip = file_from[0]
		port = int(file_from[1])
		from_path = file_from[2]
		to_path = sys.argv[2]

		if os.path.isdir(to_path):
			print "Error: path %s is a directory.  Please name the file." % to_path
			usage()

		copyFromDFS((ip, port), from_path, to_path)

	elif len(file_to) > 2:
		ip = file_to[0]
		port = int(file_to[1])
		to_path = file_to[2]
		from_path = sys.argv[1]

		if os.path.isdir(from_path):
			print "Error: path %s is a directory.  Please name the file." % from_path
			usage()

		copyToDFS((ip, port), to_path, from_path)


