from tempfile import NamedTemporaryFile
import shutil
import csv
import json

def log_transaction(filename,log):
    """
    This function is used to log a transaction.
    """

    log = json.dumps(log)
    with open(filename,'w') as csvF:
        csvWriter = csv.writer(csvF,delimiter = ' ')
        csvWriter.writerow([log])
               
def mark_transaction_complete(filename,transaction,identifier):
    """
    This function is used to mark a transaction as complete.
    """

    tempfile = NamedTemporaryFile(delete=False)
    with open(filename, 'r') as csvFile,tempfile:
        reader = csv.reader(csvFile, delimiter=' ')
        writer = csv.writer(tempfile, delimiter=' ')
        for row in reader:
            row = json.loads(row[0])
            k,_ = list(row.items())[0]
            if k == identifier:
                row[k]['completed'] = True
            row = json.dumps(row)
            writer.writerow([row])
    shutil.move(tempfile.name, filename)  
    
def seller_log(trade_list):
    """
    This function is used to log the seller information
    """

    with open('seller_info.csv','w') as csvF:
        csvWriter = csv.writer(csvF,delimiter = ' ')
        for k,v in trade_list.items():
            log = json.dumps({k:v})
            csvWriter.writerow([log])  
                    
def read_seller_log():
    """
    This function is used to log the seller log.
    """
    with open('seller_info.csv','r') as csvF:
        seller_log = csv.reader(csvF,delimiter = ' ')
        dictionary = {}
        for log in seller_log:
            log = json.loads(log[0])
            k,v = list(log.items())[0]
            dictionary[k] = v
    return dictionary
      
def change_entry(transaction,identifier):
    """
    This function will help in marking a transaction complete.
    """

    tempfile = NamedTemporaryFile(delete=False)
    with open('seller_info.csv', 'r') as csvFile,tempfile:
        reader = csv.reader(csvFile, delimiter=' ')
        writer = csv.writer(tempfile, delimiter=' ')
        for row in reader:
            row = json.loads(row[0])
            k,_ = list(row.items())[0]
            if k == identifier:
                row = transaction
            row = json.dumps(row)
            writer.writerow([row])
    shutil.move(tempfile.name, 'seller_info.csv')
   
def get_unserved_requests(file_name):
    """
    This function will help in serving the unserved requests.
    """

    with open(file_name,'r') as csvF:
        transaction_log = csv.reader(csvF,delimiter = ' ')
        open_requests = []
        transaction_list = list(transaction_log)
        last_request = json.loads(transaction_list[len(transaction_list)-1][0])
        _,v = list(last_request.items())[0]
        if v['completed'] == False:
            return last_request
        else:
            return None