import socket 
import json
from random import randrange
from zookeeper_apis import get_ports,get_server,check_active

host = "localhost" 
BUFFER_SIZE = 2000
option=1
#Obtaining the available/active servers in the beginning
available_ports = get_ports()
available_ports=available_ports[::-1]
print("Available ports",available_ports)

#randomly assigning one server as primary and next as backup from active servers
def random_assign():
    random_index = randrange(0,len(available_ports)-1)
    primary_server_id=random_index+1
    backup_server_id=((random_index+1)%3)+1
    print("PRIMARY SERVER ID:",primary_server_id)
    print("BACKUP SERVER ID:",backup_server_id)
    active_primary=check_active(available_ports[random_index])
    active_backup=check_active(available_ports[(random_index+1)%3])
    print("PRIMARY SERVER PORT:",available_ports[random_index])
    print("BACKUP SERVER PORT:",available_ports[(random_index+1)%3])
    return(active_primary,active_backup,available_ports[random_index],available_ports[(random_index+1)%3],primary_server_id,backup_server_id)

#sending the json object to the Server
def put(key,value,port,replication):
    tcpClientA = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpClientA.connect((host, port))
    a=[]
    a.append(key)
    a.append(value)
    test=[]
    test.append(1)
    test.append(a)
    c=[]
    c.append(replication)
    test.append(c)
    b = json.dumps(test).encode('utf-8')
    tcpClientA.sendall(b)
    data = tcpClientA.recv(BUFFER_SIZE)
    print (" Client2 received data:", data.decode('utf-8'))
    tcpClientA.close()


while option!=3:
	print("1. Put (key value)")
	print("2. Get (key)")
	print("3. exit")
	option=int(input("Enter option"))
    
    #Randomly choosing one server as the primary server and the next server as the backup server and putting
    #the key value pair in a json object and sending to server
	if option==1:
		key=input("Enter key: ")
		value=int(input("Enter value: "))
		active_primary,active_backup,port_primary,port_backup,primary_server_id,backup_server_id=random_assign()
		print("active",active_primary,"active_backup",active_backup)
		if((active_primary==True)and(active_backup==True)):
		    put(key,value,port_primary,-1)
		    put(key,value,port_backup,-1)
		elif((active_primary==False)and(active_backup==True)):
		    put(key,value,port_backup,primary_server_id)
		elif((active_primary==True)and(active_backup==False)):
		    put(key,value,port_primary,backup_server_id)
	
	#Sending the key to be obtained in a json object to server	
	if option==2:
		MESSAGE=str(input("tcpClientA: Enter key:"))
		port=get_server(MESSAGE)
		print("Entering to port",port)
		tcpClientA = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		tcpClientA.connect((host, port))
		test=[]
		test.append(2)
		test.append(MESSAGE)
		b = json.dumps(test).encode('utf-8')
		tcpClientA.sendall(b)
		data = tcpClientA.recv(BUFFER_SIZE)
		print (" Client2 received data:", data.decode('utf-8'))
		tcpClientA.close()
	
	#Closing connection
	if option==3:
	    MESSAGE="Exiting!"
	    test=[]
	    test.append(3)
	    test.append(MESSAGE)
	    b = json.dumps(test).encode('utf-8')
	    tcpClientA.sendall(b)
	    tcpClientA.close()
