import socket 
from threading import Thread 
from socketserver import ThreadingMixIn 
import json
from psql_apis import insert,print_all,select,select_all_keys,replicate
from zookeeper_apis import register_server,print_children,register_server_in_others,put_watch,server_exists,modify_add,modify_mappings,get_remaining_server_info
import time
import collections

dictionary={}
# Multithreaded Python server : TCP Server Socket Thread Pool
class ClientThread(Thread): 
 
    def __init__(self,ip,port): 
        Thread.__init__(self) 
        self.ip = ip 
        self.port = port 
        print("[+] New server socket thread started for " + ip + ":" + str(port)) 
 
    def run(self): 
        
        while True : 
            b=b''
            tmp = conn.recv(4096) 
            b=b+tmp
            d=json.loads(b.decode('utf-8'))
            choice=int(d[0])
            #[1,[key,value],[to-replicate]]
            if choice==1:
            	key=d[1][0]
            	value=d[1][1]
            	replicate=d[2][0]
            	dictionary[key]=value
            	#inserting key into table
            	insert(key,value,"server3",replicate)
            	#modifying mapping in zookeeper
            	modify_mappings()
            	if(server_exists("server2")==True): 
                    put_watch("server2")
            	MESSAGE="Value added!"
            	conn.send(MESSAGE.encode('utf-8'))
            #[2,<key-to-get>]
            if choice==2:
            	client_key=d[1]
            	MESSAGE=str(select(client_key,"server3"))
            	conn.send(MESSAGE.encode('utf-8'))
            	
            '''if MESSAGE == 'exit':
                break'''
            
            if choice==3:
                MESSAGE="Exiting!"
                conn.send(MESSAGE.encode('utf-8'))
                tcpServer.close()


# Multithreaded Python server : TCP Server Socket Program
TCP_IP = "localhost" 
TCP_PORT = 43759 
BUFFER_SIZE = 20
k=0
tcpServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
tcpServer.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
tcpServer.bind(('', TCP_PORT))
threads = [] 
connected_port=TCP_PORT
print("Connected port: ",TCP_PORT)

    
def server_init():
    check_master=print_children()
    if(check_master==[]):
        master=True
        print("I am the master server!")
        #Copy from backup server if any replications
        replicate("server3")
        #Obtain all keys from server3
        d={}
        keys_in_server=select_all_keys("server3") #list
        d[connected_port]=keys_in_server
        d=collections.OrderedDict(d)
        #Register self port and key-server mappings in self's zookeeper node
        register_server(d,"server3")
        time.sleep(10)
        #Register other servers' port and key-server mappings in self's zookeeper node
        get_remaining_server_info("server3")
        #If other server doesn't have port number, register info in other nodes 
        register_server_in_others(d)
    else:
        print("I am the child server!")
        print("Registering with master!")
        #Copy from backup server if any replications
        replicate("server3")
        #Obtain all keys from server3
        d={}
        keys_in_server=select_all_keys("server3") #list
        d[connected_port]=keys_in_server
        d=collections.OrderedDict(d)
        #Register self port and key-server mappings in self's zookeeper node
        register_server(d,"server3")
        time.sleep(10)
        #Register other servers' port and key-server mappings in self's zookeeper node
        get_remaining_server_info("server3")
        #If other server doesn't have port number, register info in other nodes
        register_server_in_others(d)
    

while True:
    if(k==0):
        server_init()
        k=k+1
    time.sleep(5)
    if(server_exists("server2")==True): 
        put_watch("server2") 
    print("Multithreaded Python server : Waiting for connections from TCP clients...") 
    tcpServer.listen(4) 
    (conn, (ip,port)) = tcpServer.accept() 
    newthread = ClientThread(ip,port) 
    newthread.start() 
    threads.append(newthread) 
    
 
for t in threads: 
    t.join()
