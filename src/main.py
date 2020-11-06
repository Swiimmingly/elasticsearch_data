import argparse
from sodapy import Socrata
import requests
import os
from requests.auth import HTTPBasicAuth
import json
import threading

USERNAME = os.environ['ES_USERNAME']
PASSWORD = os.environ['ES_PASSWORD']
ES_HOST = os.environ['ES_HOST']
DATASET_ID = os.environ['DATASET_ID']

def index_mapping():
    global ES_HOST, USERNAME, PASSWORD
    try:
        index = requests.put(f"{ES_HOST}/parking_violations", auth = HTTPBasicAuth(USERNAME, PASSWORD), json={
            "settings": {
                "number_of_shards": 1
                },
            "mappings": {
                "properties": {
                "plate": { "type": "keyword" },
                "state": { "type": "keyword" },
                "license_type": { "type": "keyword" },
                "summons_number": { "type": "keyword" },
                "issue_date": { "type": "date", "format":"MM/dd/yyyy" },
                "violation": { "type": "keyword" },
                "fine_amount": { "type": "float" },
                "penalty_amount": { "type": "float" },
                "interest_amount": { "type": "float" },
                "reduction_amount": { "type": "float" },
                "payment_amount": { "type": "float" },
                "amount_due": { "type": "float" },
                "precinct": { "type": "keyword" },
                "county": { "type": "keyword" },
                "issuing_agency": { "type": "keyword" }
                }
            }})
    except:
        pass

def parse_and_push(client, page_size, offset1):
    global ES_HOST, USERNAME, PASSWORD, DATASET_ID
    data = client.get(DATASET_ID, limit=page_size, offset=offset1)
    bulk=''
    action = '{ "index" : { "_index" : "parking_violations", "_type" : "_doc" } }'
    for record in data:
        try:
            del record['summons_image']
            del record['violation_time']
        except:
            pass 
        try:
            record['fine_amount'] = float(record['fine_amount'])
            record['penalty_amount'] = float(record['penalty_amount'])
            record['interest_amount'] = float(record['interest_amount'])
            record['reduction_amount'] = float(record['reduction_amount'])
            record['payment_amount'] = float(record['payment_amount'])
            record['amount_due'] = float(record['amount_due'])           
        except:
            continue
        record = json.dumps(record)
        bulk += action + '\n' + str(record) + '\n'
    try:
        requests.post(f"{ES_HOST}/parking_violations/_bulk/", auth = HTTPBasicAuth(USERNAME, PASSWORD), data=bulk, headers={'content-type':'application/json', 'charset':'UTF-8'})
    except:
        pass



def main():
    global DATASET_ID
    #define mapping
    index_mapping()

    #arguments
    parser = argparse.ArgumentParser(description="Load data into elasticsearch. ")
    
    parser.add_argument('--page_size', type=int, help='specify the number of records on each page', required=True, metavar='')
    parser.add_argument('--num_pages', type=int, help='specify the number of pages you want to load in', metavar='')
    args = parser.parse_args()

    #socrata
    API_KEY = os.environ['APP_TOKEN']
    socrata_domain = "data.cityofnewyork.us"
    client = Socrata(socrata_domain, API_KEY, timeout=120)

    counter = 0
    if args.num_pages != None:
        for num in range(args.num_pages):
            parse_and_push(client, args.page_size, num*args.page_size)
    else:
        edge = int(client.get(DATASET_ID, select='COUNT(*)')[0]['COUNT'])
        offset1 = 0
        while offset1 < edge:
            threads = []
            for i in range(10):
                offset1 = counter*args.page_size
                counter += 1
                t = threading.Thread(target=parse_and_push,args=(client,args.page_size, offset1, ),)
                threads.append(t)
                t.start()
            for th in threads:
                th.join()

    client.close()


if __name__ == "__main__":
    main()
