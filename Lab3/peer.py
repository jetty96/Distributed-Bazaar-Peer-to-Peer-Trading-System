import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import time
import threading as td
import socket,socketserver
import random
import numpy as np
import sys
import json
import csv
from tempfile import NamedTemporaryFile
import csv_operations
import os.path
from datetime import datetime

"""This class is used to ensure that there is a multi-threaded RPC server"""
class AsyncXMLRPCServer(socketserver.ThreadingMixIn,SimpleXMLRPCServer): pass

class LamportClock:
    """
    This class is used to update and adjust the lamport clock values
    """
    #Current value of the lamport clock
    value = 0

    def __init__(self, initial_value=0):
        self.value = initial_value

    def adjust(self, other):
        """
        This function is used to adjust the value of the lamport clock.
        """
        self.value = max(self.value, other)

    def forward(self):
        """
        This function is used to increment the value of the lamport clock.
        """
        self.value += 1
        return self.value

class peer:

    def __init__(self, host_addr, peer_id, neighbors, db):
        """
        Initializing the values.
        host_addr = It is a combination of both  the host IP and the port number
        peer_id = The unique ID of each of the peer in the network
        neighbors = The different neighbors for the peer in the network
        db = The database server process
        """

        self.host_addr = host_addr
        self.peer_id = peer_id
        host_ip = socket.gethostbyname(socket.gethostname())
        self.db_server = host_ip + ':8057'
        self.neighbors = neighbors
        self.db = db 
        self.trader = []
       
        #Various flags
        self.didReceiveOK = False 
        self.didReceiveWon = False 
        self.didSendWon = False
        self.trade_list = {} 
        
        self.worker = self.create()
        self.heartbeat_reply = False

        #Lamport Clock.
        self.lamport_clock = LamportClock()
       
        #The Semaphores
        self.flag_won_semaphore = td.BoundedSemaphore(1) 
        #Semaphore for the list of the trading products available.
        self.trade_list_semaphore = td.BoundedSemaphore(1)
        #Semaphore for Altering the List of the traders.
        self.semaphore = td.BoundedSemaphore(1)
        # Semaphore For Clock.
        self.clock_semaphore = td.BoundedSemaphore(1)
        self.heartbeat_reply_semaphore = td.BoundedSemaphore(1)     
        

    def get_rpc(self,neighbor):
        """
        This function helps in returning the proxy for a particular address
        neighbor - Neighbor for the peer
        """

        a = xmlrpc.client.ServerProxy('http://' + str(neighbor) + '/')
        try:
            #Calling the fictive method
            a.test()   
        except xmlrpc.client.Fault:
            #Connected to the server and the method doesn't exist which is expected.
            pass
        except socket.error:
            #Not connected ; socket error mean that the service is unreachable.
            return False, None
            
        #In case the method is registered in the XmlRPC server
        return True, a
        
   
    def startServer(self):
        """
        This function is used to start the server and then register all of the functions.
        """

        host_ip = socket.gethostbyname(socket.gethostname())
        server = AsyncXMLRPCServer((host_ip,int(self.host_addr.split(':')[1])),allow_none=True,logRequests=False)
        server.register_function(self.lookup,'lookup')
        server.register_function(self.transaction,'transaction')
        server.register_function(self.election_message,'election_message')
        server.register_function(self.register_products,'register_products')
        server.register_function(self.adjust_buyer_clock,'adjust_buyer_clock')
        server.register_function(self.election_restart_message,'election_restart_message')
        server.register_function(self.sync,'sync')
        server.register_function(self.ping_message,'ping_message')
        server.register_function(self.ping_reply,'ping_reply')
        server.register_function(self.trader_status_update,"trader_status_update")
        server.serve_forever()

    def election_restart_message(self):
        """
        This function is called where the peer recieves an election message and indicates that the new election has been started and then it will update the election flag. This flag also indicates that if any buyer is interested in buying to wait for the election process to be completed.
        """
        self.flag_won_semaphore.acquire()
        self.didReceiveOK = False
        self.didReceiveWon = False
        if self.peer_id == 1:
            time.sleep(3.0)
            thread = td.Thread(target=self.start_election,args=())
            thread.start()
        self.flag_won_semaphore.release()
        
    
    def send_restart_election_messages(self,_,neighbor):
        """
        This function is used to send the election restart message to the peers.
        neighbor - neighbors of the peer
        """
        connected,proxy = self.get_rpc(neighbor['host_addr'])
        if connected:
            proxy.election_restart_message() 
            
              
    def send_message(self,message,neighbor):
        """
        This function is used to send the election message to the peers
        message - the message to be displayed
        neighbor - neighbors of the peer
        """
        connected,proxy = self.get_rpc(neighbor['host_addr'])
        if connected:
            proxy.election_message(message,{'peer_id':self.peer_id,'host_addr':self.host_addr,'status':1})
            
    def fwd_won_message(self):
        """
        This function is used to send the flags and also the I won message to all the peers.
        """

        #To get the date and the time
        date_time = datetime.now()
        date_time_str = date_time.strftime("%d.%m.%Y %H:%M:%S")

        print  (date_time_str , "Dear buyers and sellers, My ID is ",self.peer_id, "and I am the new coordinator")
        self.didReceiveWon = True
        
        self.trader.append({'peer_id':self.peer_id,'host_addr':self.host_addr,'status' : 1})
        self.db['Role'] = 'Trader'
        self.flag_won_semaphore.release()
        for neighbor in self.neighbors:
            thread = td.Thread(target=self.send_message,args=("I won",neighbor)) 
            thread.start() 
       
        if len(self.trader) == 1:
             time.sleep(3.0)
             for neighbor in self.neighbors:
                thread = td.Thread(target = self.send_restart_election_messages,args = (" ",neighbor))
                thread.start()
        else:
            print ("Let is begin Trading")
            thread2 = td.Thread(target=peer_local.begin_trading,args=())
            thread2.start()                 
            
    def election_message(self,message,neighbor):
        """
        This function is used to  handle the three types of messages. They are:
        1. election  - when we receive this message the peer send a OK message and if there are any higher peers then it will forward the messgae and waits for the OK messages and if doesn't receive any OK messages it will become the leader.
        2. OK - It will drop out from the election and will set the flag didReceiveOK and it will also prevent from further forwarding the election messages.
        3. I won - When it receives this message the peer wills et the leader details to a variable where the trader will start the trading.

        messgae - the message to be displayed
        neighbor - the neighboring peers
        """

        #To get the date and the time
        date_time = datetime.now()
        date_time_str = date_time.strftime("%d.%m.%Y %H:%M:%S")

        #Forward the election to higher peers, if available. Response here are Ok and I won.
        if message == "election":
            if self.didReceiveOK or self.didReceiveWon:
                thread = td.Thread(target=self.send_message,args=("OK",neighbor))
                thread.start()
            else:
                thread = td.Thread(target=self.send_message,args=("OK",neighbor))
                thread.start()
                peers = [x['peer_id'] for x in self.neighbors]
                peers = np.array(peers)
                x = len(peers[peers > self.peer_id])

                if x > 0:
                    self.flag_won_semaphore.acquire()
                    #Setting the flag
                    self.isElectionRunning = True
                    self.flag_won_semaphore.release()
                    self.didReceiveOK = False
                    for neighbor in self.neighbors:
                        if neighbor['peer_id'] > self.peer_id:
                            if self.trader != [] and neighbor['peer_id'] == self.trader[0]['peer_id']:
                                pass
                            else:    
                                thread = td.Thread(target=self.send_message,args=("election",neighbor))
                                thread.start()
                    #Time out           
                    time.sleep(2.0)
                                            
                    self.flag_won_semaphore.acquire()
                    if self.didReceiveOK == False and self.didSendWon == False:
                        self.didSendWon = True
                        #Release of semaphore is done by that method.
                        self.fwd_won_message() 
                    else:
                        self.flag_won_semaphore.release()
                               
                elif x == 0:
                    self.flag_won_semaphore.acquire()
                    if self.didSendWon == False:
                        self.didSendWon = True
                        self.fwd_won_message()
                    else:
                        self.flag_won_semaphore.release()
                        
        elif message == 'OK':
            #Drop out and wait
            self.didReceiveOK = True
                            
        elif message == 'I won':
            print (date_time_str, "The Peer ID ",self.peer_id," has received the Election Won Msg")
            self.flag_won_semaphore.acquire()
            self.didReceiveWon = True
            self.flag_won_semaphore.release()
            self.trader.append(neighbor)
            time.sleep(3.0)

            #Once Both the traders are elected, start the trading process.
            if len(self.trader) == 2: 
                thread2 = td.Thread(target=peer_local.begin_trading,args=())
                thread2.start()
    
    def start_election(self):
        """
        This function will start the election and will forward the election message to all of its peers. If there are no higher peers then it will become the leader and then send the "I Won" message to all of the peers.
        """

        #To get the date and the time
        date_time = datetime.now()
        date_time_str = date_time.strftime("%d.%m.%Y %H:%M:%S")

        print (date_time_str, "The Peer ID ",self.peer_id," has Started the election")

        time.sleep(1)

        #Check number of peers higher than you.
        peers = [x['peer_id'] for x in self.neighbors]
        peers = np.array(peers)
        x = len(peers[peers > self.peer_id])
        if x > 0:
            self.didReceiveOK = False
            self.didReceiveWon = False
            for neighbor in self.neighbors:
                if neighbor['peer_id'] > self.peer_id:

                    #Don't send it to previous trader as he is dead.
                    if self.trader != [] and neighbor['peer_id'] == self.trader[0]['peer_id']: 
                        pass
                    else:    
                        thread = td.Thread(target=self.send_message,args=("election",neighbor))
                        thread.start()  
            time.sleep(2.0)
            self.flag_won_semaphore.acquire()
            if self.didReceiveOK == False and self.didReceiveWon == False:
               self.didSendWon = True
               self.fwd_won_message()
            else:
                self.flag_won_semaphore.release()
        else: 
            #No higher peers
            self.flag_won_semaphore.acquire()
            self.didSendWon = True
            #Release of semaphore is in fwd_won_message
            self.fwd_won_message() 
     
    def get_active_trader(self):
        """
        This fucntion helps in getting the active trader and will also check the status of the trader and will return the active trader proxy.
        """
        self.semaphore.acquire()
        x = random.randint(0, 1)
        trader = self.trader[x]
        if not trader["status"]:
            z = [0,1]
            z.remove(x)
            x = z[0]
            trader = self.trader[x]
        self.semaphore.release()
        return self.get_rpc(self.trader[x]["host_addr"])
         
    def begin_trading(self):
        """
        This function will help the seller in which they will register their product to the trader. In the case of the buyer they will start the lookup process for the products that are required and each of the lookup process is directed to the trader and trader helps in the transaction between the buyer and the sellers.
        """

        #To get the date and the time
        date_time = datetime.now()
        date_time_str = date_time.strftime("%d.%m.%Y %H:%M:%S")

        #Delay so that all the election message are replied or election is dropped by peers other than the trader and then reset the flags.
        self.didReceiveWon = False
        self.didReceiveOK = False

        #If Seller then he will register the poducts.
        if self.db["Role"] == "Seller":
            connected,proxy = self.get_active_trader()
            p_n = None
            p_c = None
            for product_name, product_count in self.db['Inv'].items():
                if product_count > 0:
                    p_n= product_name
                    p_c = product_count
            seller_info = {'seller_id': {'peer_id':self.peer_id,'host_addr':self.host_addr},'product_name':p_n,'product_count':p_c} 
            if connected:
                proxy.register_products(seller_info)
        elif self.db["Role"] == "Trader":
            connected,proxy = self.get_rpc(self.db_server)

            #Connect with the database
            if connected: 
                proxy.register_traders({'peer_id':self.peer_id,'host_addr':self.host_addr})
            self.worker.start()

        #In case of buyer then we wait for 2 sec for seller to register products and then start buying.
        elif self.db["Role"] == "Buyer":
            #Allow the sellers to register the products.
            time.sleep(3.0 + self.peer_id/10.0) 
            
            while len(self.db['shop'])!= 0:
                item = self.db['shop'][0]
                connected,proxy = self.get_active_trader()
                if connected:
                    self.clock_semaphore.acquire()
                    self.lamport_clock.forward()
                    request_ts = self.lamport_clock.value
                    self.broadcast_lamport_clock()
                    self.clock_semaphore.release()
                    print (date_time_str, " The Peer ID ",self.peer_id, "has issued a lookup request to the trader for the item ",item)
                    proxy.lookup({'peer_id':self.peer_id,'host_addr':self.host_addr},item,request_ts)       
                    self.db['shop'].remove(item)                   
                    time.sleep(3.0)           
            
    def broadcast_lamport_clock(self):
        """
        This function is used to broadcast the peer clock to all the peers
        """

        for neighbor in self.neighbors:
            thread = td.Thread(target=self.send_broadcast_message,args=("I won",neighbor))
            thread.start()
            
    def send_broadcast_message(self,_,neighbor): 
        """
        This function is used to broadcast the peer clock to all the peers
        """

        connected,proxy = self.get_rpc(neighbor['host_addr'])
        if connected:
            proxy.adjust_buyer_clock(self.lamport_clock.value) 
    
    def adjust_buyer_clock(self, other):
        """
        This function will help the peer in adjusting the clock only if it a buyer or a seller.
        """

        #To get the date and the time
        date_time = datetime.now()
        date_time_str = date_time.strftime("%d.%m.%Y %H:%M:%S")

        #Trader should not adjust his clock until he recives the lookup request.
        if self.db["Role"] == "Buyer" or self.db["Role"] != "Trader": 
            self.clock_semaphore.acquire()
            self.lamport_clock.adjust(other) 
            self.clock_semaphore.release()

    def register_products(self,seller_info):
        """
        This function is used at the trader end where it will register the items with the trader.
        """

        #Key in trade-list
        seller_peer_id = seller_info['seller_id']['peer_id']
        #Add the product in local cache and contact DB to update this info.
        self.trade_list[str(seller_peer_id)] = seller_info 
        connected,proxy = self.get_rpc(self.db_server)
        if connected:
            proxy.register_products(seller_info,{'peer_id':self.peer_id,'host_addr':self.host_addr})
      
    def lookup(self,buyer_id,product_name,buyer_clock):
        """
        This function is used for the trader to lookup for the items that he wants to buy and then perform the transaction between the buyer and the seller.
        """

        self.lamport_clock.adjust(buyer_clock)        
        seller_list = []
        transaction_file_name =  "transactions_" + str(self.peer_id) + ".csv"
        for peer_id,seller_info in self.trade_list.items():

            #Find all the seller who sells the product
            if seller_info["product_name"] == product_name: 
                seller_list.append(seller_info)
                
        if len(seller_list) > 0:
            
            #Log the request and then choose the first seller
            seller = seller_list[0] 
            transaction_log = {str(self.lamport_clock.value) : {'product_name' : product_name, 'buyer_id' : buyer_id, 'seller_id':seller['seller_id'],'completed':False}}
            csv_operations.log_transaction(transaction_file_name,transaction_log)
            
            connected, proxy = self.get_rpc(self.db_server)
            if connected: # Contact DB Server for the transaction to complete.
                proxy.transaction(product_name,seller)

            # Reply to buyer that transaction is succesful. 
            connected,proxy = self.get_rpc(buyer_id["host_addr"])
            if connected: # Pass the message to buyer that transaction is succesful
                proxy.transaction(product_name,seller['seller_id'],buyer_id,self.peer_id)
                
            connected,proxy = self.get_rpc(seller['seller_id']["host_addr"])
            if connected:# Pass the message to seller that its product is sold
                proxy.transaction(product_name,seller['seller_id'],buyer_id,self.peer_id)
    
    def transaction(self, product_name, seller_id, buyer_id,trader_peer_id):
        """
        This function is used to perform the transaction where the seller will deduct the item count from the seller
        """

        #To get the date and the time
        date_time = datetime.now()
        date_time_str = date_time.strftime("%d.%m.%Y %H:%M:%S")

        if self.db["Role"] == "Buyer":
            print  (date_time_str , "The Peer ID ", self.peer_id, " has bought the item ",product_name, " from the seller with the ID ",seller_id["peer_id"]," via the trader ID ",trader_peer_id)
        elif self.db["Role"] == "Seller":
            self.db['Inv'][product_name] = self.db['Inv'][product_name] - 1

            if self.db['Inv'][product_name] == 0:
                #Pickup a random item and register that product with trader.
                product_list = ['Fish','Salt','Boar']
                y = random.randint(0, 2)
                random_product = product_list[y]
                self.db['Inv'][random_product] = 3
                seller_info = {'seller_id': {'peer_id':self.peer_id,'host_addr':self.host_addr},'product_name':random_product,'product_count':3}
                connected,proxy = self.get_active_trader() 
                if connected: 
                    proxy.register_products(seller_info)
                    
    def sync(self,seller_info):
        """
        This function is used to perform the syncing.
        """
        self.trade_list_semaphore.acquire()
        seller_peer_id = seller_info['seller_id']['peer_id']
        self.trade_list[str(seller_peer_id)] = seller_info 
        self.trade_list_semaphore.release()   
        
    def create(self):
        """
        This function is used in creating a thread.
        """
        thr = td.Thread(target=self.ping_timer)
        return thr
        
    def ping_timer(self):
        """
        This function is used to ping the timer.
        """

        #To get the date and the time
        date_time = datetime.now()
        date_time_str = date_time.strftime("%d.%m.%Y %H:%M:%S")

        other_trader = [trader for trader in self.trader if not trader['peer_id'] == self.peer_id][0]

        #Initial stop condition
        stop_condition = False

        while(stop_condition == False):
            # et the reply flag as false
            self.heartbeat_reply = False 
            connected,proxy = self.get_rpc(other_trader["host_addr"])
            if connected:
                proxy.ping_message({'peer_id':self.peer_id,'host_addr':self.host_addr,'status':1})

            #Time-out.        
            time.sleep(2.0)  
            self.heartbeat_reply_semaphore.acquire()

            #Check whether the reply is received.
            if not self.heartbeat_reply: 
                self.heartbeat_reply_semaphore.release()
                stop_condition = True
            else:
                self.heartbeat_reply_semaphore.release()
                time.sleep(5.0)
                
        #When the trader is down then we will broadcast this it all the peers.
        for neighbor in self.neighbors:
            connected,proxy = self.get_rpc(neighbor['host_addr'])

            if connected:
                #When the trader is down.
                proxy.trader_status_update(False,other_trader)
                
        #Read the logs and perform the operations.
        file_name = "transactions_" + str(other_trader['peer_id']) + ".csv"
        if os.path.isfile(file_name):
            unserved_requests = csv_operations.get_unserved_requests(file_name)
            if unserved_requests is None:
                print ("None")
                pass
            else:
                for unserved_request in unserved_requests:
                    k,v  = unserved_request.items()[0]
                    self.lookup(v['buyer_id'],v['product_name'],int(k))
                         
    def trader_status_update(self,status,trader):
        """
        This function is used to check the trader status and update the status.
        """

        self.semaphore.acquire()
        for x in range(len(self.trader)):
            if self.trader[x]['peer_id'] == trader['peer_id']:

                # Update the status of the trader as false.
                self.trader[x]['status'] = False 
        self.semaphore.release()
        print ("The status of ",trader['peer_id'],"is ",status)
                            
    def ping_message(self,trader_info):
        """
        This function is used to send a ping message.
        """
        connected,proxy = self.get_rpc(trader_info["host_addr"])
        if connected:
            proxy.ping_reply({'peer_id':self.peer_id,'host_addr':self.host_addr,'status':1})
        
    def ping_reply(self,trader_info):
        self.heartbeat_reply_semaphore.acquire()
        self.heartbeat_reply = True
        self.heartbeat_reply_semaphore.release()
        
if __name__ == "__main__":
    host_ip = socket.gethostbyname(socket.gethostname())
    host_addr = host_ip + ":" + sys.argv[2]
    peer_id = int(sys.argv[1])
    db = json.loads(sys.argv[3])
    num_peers = int(sys.argv[4])
    
    #Computing the neigbors
    peer_ids = [x for x in range(1,num_peers+1)]
    host_ports = [(10007 + x) for x in range(0,num_peers)]
    host_addrs = [(host_ip + ':' + str(port)) for port in host_ports]
    neighbors = [{'peer_id':p,'host_addr':h} for p,h in zip(peer_ids,host_addrs)]
    neighbors.remove({'peer_id':peer_id,'host_addr':host_addr})
    
    #Declare a peer variable and start it.  
    peer_local = peer(host_addr,peer_id,neighbors,db)
    thread1 = td.Thread(target=peer_local.startServer,args=())
    thread1.start()

    # Starting the election, lower peers.
    if peer_id <= 2:
        thread1 = td.Thread(target=peer_local.start_election,args=())
        thread1.start()