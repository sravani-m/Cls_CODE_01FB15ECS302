from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.protocol.states import EventType
import os
import time
import subprocess
import json
import ast
from collections import OrderedDict
from psql_apis import select_all_keys

#Putting watch on given server
def put_watch(server_name):
    print("--- Putting watch on server "+server_name+" ---")
    children = zk.get("/big_data_assign/"+server_name,watch=watch_host)
	    
def watch_host(event):
	print("change!!")
	print(event)
	if(event.type=="DELETED"):
	    print("do something")
	    modify_mappings()
	    return -1
	if(event.type=="CHANGED"):
	    a=(event.path).split('/')
	    put_watch(a[2])
	    return -1

#Modify key-server mappings in zookeeper
def modify_mappings():
    children = zk.get_children("/big_data_assign")
    for each in children:
        path="/big_data_assign/"+each
        print(path)
        c = zk.get(path)
        c=c[0]
        c=c.decode()
        c = eval(c, {'OrderedDict': OrderedDict})
        key=list(c.items())[0]
        key=key[0]
        d={}
        d[key]=select_all_keys(each)
        d=OrderedDict(d)
        modify_server(d,each)
    for each in children:
        get_remaining_server_info(each)      


#Modify key-server of given server mappings
def modify_server(port_key_mapping,server_name):
	print("Registering server with zookeeper... ")
	zk.ensure_path("/big_data_assign")
	port_key_mapping=str(port_key_mapping)
	port_key_mapping=port_key_mapping.encode()
	# Registering program ID in ephemeral node in zookeeper
	zk.get_children("/big_data_assign/")
	path="/big_data_assign/"+server_name
	zk.set(path,port_key_mapping)


#Noting state of zookeeper        
def my_listener(state):
    if state == KazooState.LOST:
    # Register somewhere that the session was lost
        print("Lost")
    elif state == KazooState.SUSPENDED:
    # Handle being disconnected from Zookeeper
        print("Suspended")
    else:
    # Handle being connected/reconnected to Zookeeper
    	print("Being Connected/Reconnected")


#port_key_mappings - {port:[list of keys]}
def register_server(port_key_mapping,server_name):
	print("Registering server with zookeeper... ")
	zk.add_listener(my_listener)
	zk.ensure_path("/big_data_assign")
	port_key_mapping=str(port_key_mapping)
	port_key_mapping=port_key_mapping.encode()
	# Registering program ID in ephemeral node in zookeeper
	zk.get_children("/big_data_assign/")
	path="/big_data_assign/"+server_name
	zk.create(path,value=bytes(port_key_mapping),acl=None,ephemeral=True,makepath=True)
	return 1

#Check if server corresponding to the port is active
def check_active(port):
    zk.ensure_path("/big_data_assign")
    children = zk.get_children("/big_data_assign")
    path="/big_data_assign/"
    for each in children:
        path_child=path+each
        c = zk.get(path_child)
        c=c[0]
        c=c.decode()
        c = eval(c, {'OrderedDict': OrderedDict})
        key=list(c.items())[0]
        key=key[0]
        if(key==port):
            active=True
            break;
        else:
            active=False
    return active
                
#register server mappings in other server information    
def register_server_in_others(port_key_mapping):
    for key_to_be_added in port_key_mapping:
        print(key_to_be_added)
    value_to_be_added=port_key_mapping[key_to_be_added]
    print("Registering server with others... ")
    zk.add_listener(my_listener)
    zk.ensure_path("/big_data_assign")
    children = zk.get_children("/big_data_assign")
    path="/big_data_assign/"
    for each in children:
        path_child=path+each
        c = zk.get(path_child)
        c=c[0]
        c=c.decode()
        c = eval(c, {'OrderedDict': OrderedDict})
        flag=False
        for key, value in c.items():
            if(key==key_to_be_added):
                flag=True
                break
        if(flag!=True):
            path_child="/big_data_assign/"+each
            children = zk.get(path_child)
            children=children[0]
            children=children.decode()
            children = eval(children, {'OrderedDict': OrderedDict})
            children[key_to_be_added]=value_to_be_added
            children=str(children)
            children=children.encode()
            zk.set("/big_data_assign/"+each,children)
        
#print all nodes
def print_children():
	zk.add_listener(my_listener)
	zk.ensure_path("/big_data_assign")
	children = zk.get_children("/big_data_assign")
	print("children: ",children)
	return children

#modifying zookeeper mappings when key is added
def modify_add():
    children=zk.get_children("/big_data_assign")
    for each in children:
        path="/big_data_assign/"+each
        c = zk.get(path)
        c=c[0]
        c=c.decode()
        c = eval(c, {'OrderedDict': OrderedDict})
        key=list(c.items())[0]
        key=key[0]
        print(key)
        d={}
        d[key]=select_all_keys(each)
        d=OrderedDict(d)
        d=str(d)
        zk.set(path,d)
    
#Get active ports from zookeeper
def get_ports():
    zk.add_listener(my_listener)
    zk.ensure_path("/big_data_assign")
    children = zk.get_children("/big_data_assign")
    available_ports=[]
    for each in children:
        seq="/big_data_assign/"+each
        avail_port=zk.get(seq)
        print(avail_port)
        avail_port=avail_port[0]
        avail_port=avail_port.decode()
        avail_port = eval(avail_port, {'OrderedDict': OrderedDict})
        #first element of Ordered Dict
        a=list(avail_port.items())[0]
        print("heree",a[0],type(a[0]))
        available_ports.append(a[0])
    return available_ports

#Gives path of master server        
def master_server():
    zk.add_listener(my_listener)
    zk.ensure_path("/big_data_assign")
    children = zk.get_children("/big_data_assign")
    master=children[0]
    path="/big_data_assign/"+master
    print("master server path",path)
    return path

#Check if server is active or not
def server_exists(server_name):
    #zk = KazooClient("localhost:2181")
    #zk.start()
    children=zk.get_children("/big_data_assign")
    if(server_name not in children):
        return False
    else:
        return True
    
#Register slave servers in zookeeper
def register_children(path,keys_in_server,connected_port):
	print("Registering server with zookeeper... ")
	zk.add_listener(my_listener)
	zk.ensure_path(path)
	child_path=path+"/node"
	# Registering program ID in ephemeral node in zookeeper
	'''keys_in_server=json.dumps(keys_in_server).encode('utf-8')'''
	d=dict()
	d[connected_port]=keys_in_server
	d=str(d)
	zk.create(child_path,value=bytes(d,"utf-8"),acl=None,ephemeral=True,sequence=True,makepath=True)	    

#Get port number and key-server mappings of the active servers apart from the given server
def get_remaining_server_info(server_name):
    current_server_info=zk.get("/big_data_assign/"+server_name)
    current_server_info=current_server_info[0]
    current_server_dict=eval(current_server_info,{'OrderedDict':OrderedDict})

    a=zk.get_children("/big_data_assign")
    others=[]
    for i in a:
        print(i)
        if(i!=server_name):
            others.append(i)
    for each in others:
        path_child="/big_data_assign/"+each
        print(path_child)
        children = zk.get(path_child)
        children=children[0]
        children=children.decode()
        children = eval(children, {'OrderedDict': OrderedDict})
        for key,value in children.items():
            current_server_dict[key]=value
    current_server_dict=str(current_server_dict)
    current_server_dict=current_server_dict.encode()
    zk.set("/big_data_assign/"+server_name,current_server_dict)

#Get server port from server-key mappings for the appropriate key
def get_server(entered_key):
    zk.add_listener(my_listener)
    zk.ensure_path("/big_data_assign")
    children = zk.get_children("/big_data_assign")
    master_server=children[0]
    master_info = zk.get("/big_data_assign/"+master_server)
    #byte string
    key_mappings=master_info[0]
    #string
    key_mappings=key_mappings.decode()
    #Ordered Dict
    key_mappings = eval(key_mappings, {'OrderedDict': OrderedDict})
    for port,associated_keys in key_mappings.items():
        for each in associated_keys:
            if each==entered_key:
                required_port=port        
    return required_port
zk = KazooClient("localhost:2181")
zk.start()
