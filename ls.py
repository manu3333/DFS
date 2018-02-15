###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	List client for the DFS
#



import socket
import sys

from Packet import *

def usage():
	print """Usage: python %s <server>:<port, default=8000>""" % sys.argv[0] 
	sys.exit(0)

def client(ip, port):

	# Contacts the metadata server and ask for list of files.

	socko = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	socko.connect((ip, port))
	
	packet = Packet()
	packet.BuildListPacket()
	data_message = packet.getEncodedPacket()
	socko.sendall(data_message)

	rec = socko.recv(4096) 
	dec_packet = Packet()
	dec_packet.DecodePacket(rec)

	print_files = dec_packet.getFileArray()


	socko.close()

	for n, s in print_files:
		print """%s %d bytes"""% (n, s)

if __name__ == "__main__":

	if len(sys.argv) < 2:
		usage()

	ip = None
	port = None 
	server = sys.argv[1].split(":")
	if len(server) == 1:
		ip = server[0]
		port = 8000
	elif len(server) == 2:
		ip = server[0]
		port = int(server[1])

	if not ip:
		usage()

	client(ip, port)
