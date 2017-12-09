import psycopg2

#Insert key,value,replication into server_name
def insert(key,value,server_name,replication):
    conn = None
    try:
        conn = psycopg2.connect(host="localhost",database="bigdata",user="srav7",password="pwd")
        cur=conn.cursor()
        query="""insert into """+server_name+"""(key,value,replication) values(%s,%s,%s)"""
        cur.execute(query,(key,value,replication))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#Print all information related to a server    	    
def print_all(server_name):
    conn = None
    try:
        conn = psycopg2.connect(host="localhost",database="bigdata",user="srav7",password="pwd")
        cur=conn.cursor()
        cur.execute("""select *from """+server_name)
        rows = cur.fetchall()
        for row in rows:
            print("Key: ",row[0])
            print("Value: ",row[1])
            print("----------")
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#Select all information from a server having a particular key
def select(key,server_name):
    conn = None
    try:
        conn = psycopg2.connect(host="localhost",database="bigdata",user="srav7",password="pwd")
        cur=conn.cursor()
        query="""select *from """+server_name+""" where key="""+"'"+key+"'"
        cur.execute(query)
        rows = cur.fetchall()
        conn.commit()
        cur.close()
        print(rows)
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
#Select all keys of a particular server
def select_all_keys(server_name):
    conn = None
    try:
        conn = psycopg2.connect(host="localhost",database="bigdata",user="srav7",password="pwd")
        cur=conn.cursor()
        query="""select key from """+server_name+""";"""
        cur.execute(query)
        rows = cur.fetchall()
        conn.commit()
        cur.close()
        keys_list=[]
        for each_key_tuple in rows:
            keys_list.append(each_key_tuple[0])
        return keys_list
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#Selecting replication field from a particular server
def select_replication(server_name,replication):
    conn = None
    try:
        conn = psycopg2.connect(host="localhost",database="bigdata",user="srav7",password="pwd")
        cur=conn.cursor()
        query="""select key,value from """+server_name+""" where replication="""+str(replication)+""";"""
        cur.execute(query)
        rows = cur.fetchall()
        conn.commit()
        cur.close()
        return(rows)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#Setting replication field to -1 for all fields having a particular replication            
def set_replication(server_name,replication):
    conn = None
    try:
        conn = psycopg2.connect(host="localhost",database="bigdata",user="srav7",password="pwd")
        cur=conn.cursor()
        query="""update """+server_name+""" set replication=-1 where replication="""+str(replication)+""";"""
        cur.execute(query)
        conn.commit()
        cur.close()
        return(1)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#Selecting fields which have a particular replication ID and then setting it to -1                                    
def replicate(server):
    if(server=="server1"):
        data=select_replication("server2",1)
        data2=select_replication("server3",1)
        set_replication("server2",1)
        set_replication("server3",1)
    elif(server=="server2"):
        data=select_replication("server3",2)
        data2=select_replication("server1",2)
        set_replication("server3",2)
        set_replication("server1",2)
    elif(server=="server3"):
        data=select_replication("server1",3)
        data2=select_replication("server2",3)
        set_replication("server1",3)
        set_replication("server2",3)
    
    if((data!=[])or(data2!=[])):
        for i in data:
            key,value=i
            print(key,value)
            insert(key,value,server,-1)
        for i in data2:
            key,value=i
            print(key,value)
            insert(key,value,server,-1)
    return data,data2
