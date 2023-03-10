import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import threading as td
import socket,socketserver
import numpy as np
import sys
import json
import csv
import csv_operations
import os.path
import csv_operations
from datetime import datetime

"""This class is used to ensure that there is a multi-threaded RPC server"""
class AsyncXMLRPCServer(socketserver.ThreadingMixIn,SimpleXMLRPCServer): pass

class database_process:
    """
    Initializing the values.
    host_addr = It is a combination of both  the host IP and the port number
    peer_id = The unique ID of each of the peer in the network
    neighbors = The different neighbors for the peer in the network
    db = The database server process
    """

    def __init__(self,host_addr):
        self.host_addr = host_addr
        self.traders_info = []
        self.inventory_by_seller = {}
        self.semaphore = td.BoundedSemaphore(1)
        self.inventory_by_product = {}
        
    def get_rpc(self,neighbor):
        """
        This function is used to return the proxy of the specified address.
        """
        a = xmlrpc.client.ServerProxy('http://' + str(neighbor) + '/')
        try:
            #Call the fictive method
            a.test()  
        except xmlrpc.client.Fault:
            #Connected to the server and the method doesn't exist which is expected.
            pass
        except socket.error:
            #Not connected ; socket error mean that the service is unreachable.
            return False, None
            
        #Just in case the method is registered in the XmlRPC server
        return True, a
   
    def register_traders(self,trader_info):
        """
        This function is used to register the traders.
        """

        if not trader_info in self.traders_info:
            self.traders_info.append(trader_info)     
     
    def register_products(self,seller_info,trader_info):
        """
        This function is used to register the products into the database.
        """
        self.semaphore.acquire()
        seller_peer_id = seller_info['seller_id']['peer_id']
        self.inventory_by_seller[str(seller_peer_id)] = seller_info
        
        #Register this in inventory by product
        if seller_info['product_name'] in self.inventory_by_product:
            self.inventory_by_product[seller_info['product_name']]['product_count'] += seller_info['product_count']
        else:
            self.inventory_by_product[seller_info['product_name']] = {'product_count' : seller_info['product_count']}
        
        #Record This Info in a CSV File.
        csv_operations.seller_log(self.inventory_by_seller)
        
        #Push change to other trader.
        for trader in self.traders_info:
            if not trader['peer_id'] is trader_info['peer_id']:
                self.push_changes(trader,seller_info)
                
        self.semaphore.release()
             
    def lookup(self,product_name):
        """
        This fucntion is used to perform the lookup function and will return the product count.
        """
        count = 0
        self.semaphore.acquire()

        #Returns the product count
        count = self.inventory_by_product[product_name]
        self.semaphore.release() 
        return count
        
    
    def push_changes(self,trader_info,seller_info):
        """
        This function is used to push the change to the respective trader.
        """ 

        connected,proxy = self.get_rpc(trader_info['host_addr'])
        if connected:
            proxy.sync_cache(seller_info)
    
    def transaction(self,product_name,seller_info): 
        """
        This function is used to deduct the item quantity and then update the changes.
        """

        self.semaphore.acquire()
        seller_peer_id = str(seller_info['seller_id']['peer_id'])
        self.inventory_by_seller[seller_peer_id]["product_count"] = self.inventory_by_seller[seller_peer_id]["product_count"] -1
        self.inventory_by_product[product_name]['product_count'] -= 1
        csv_operations.change_entry(self.inventory_by_seller[seller_peer_id], seller_peer_id)
        
        for trader in self.traders_info:
            seller_peer_id = seller_info['seller_id']['peer_id']
            new_info = self.inventory_by_seller[str(seller_peer_id)]
            self.push_changes(trader,new_info)
        self.semaphore.release()
        
    def startServer(self):

        #To get the date and the time
        date_time = datetime.now()
        date_time_str = date_time.strftime("%d.%m.%Y %H:%M:%S")

        print ("Started the server")
        host_ip = socket.gethostbyname(socket.gethostname())
        server = AsyncXMLRPCServer((host_ip,int(self.host_addr.split(':')[1])),allow_none=True,logRequests=False)
        server.register_function(self.lookup,'lookup')
        server.register_function(self.transaction,'transaction')
        server.register_function(self.register_products,'register_products')
        server.register_function(self.register_traders,'register_traders')
        server.serve_forever()
        
if __name__ == "__main__":
    host_ip = socket.gethostbyname(socket.gethostname())
    host_addr = host_ip + ":" + sys.argv[1]
    db_server = database_process(host_addr)
    thread = td.Thread(target=db_server.startServer,args=())
    thread.start()