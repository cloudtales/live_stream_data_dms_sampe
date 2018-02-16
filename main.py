import boto3
import json
import datetime
import calendar
import random
import time
import cx_Oracle
import configparser
import pandas as pd


def random_date(start,l):
    current = start
    while l >= 0:
        current = current + datetime.timedelta(minutes=random.randrange(60))        
        yield current
        l-=1
    
def generate_synthetic_payload(df):
    batch = []
    collection = []
    
    rnd_events = ['entered_rest_rooms','bought_a_beer','entered_smoker_lounge']
        
    for index, row in df.iterrows():
        ticket_id = row ['ID']
        start_time = datetime.datetime(2013, 9, 20,13,00) 
        
        no_of_events = random.randint(0,3)
        event_dates = random_date(start_time,no_of_events + 2)            
            
        events_for_ticket_id = ['signed_in']
        events_for_ticket_id = events_for_ticket_id + random.sample(rnd_events,no_of_events) 
        events_for_ticket_id.append('signed_out')
        
        for event in events_for_ticket_id:
            payload = {
                'ticketid': ticket_id,
                'date': next(event_dates).strftime("%d/%m/%y %H:%M"),
                'status': event
            }
            if len(batch) <= 499:
                batch.append({'Data': json.dumps(payload) + '\n'})
            else:
                collection.append(list(batch))
                del batch[:]
    if batch:
        collection.append(list(batch))
        
    return collection

def send_batch_to_firehose(stream, batch):
   put_response = kinesis_client.put_record_batch(
        DeliveryStreamName=stream,
        Records= batch)
     

# Load config.in
config = configparser.ConfigParser()
config.read('config.ini')

# Create boto3 session & kinesis client
session = boto3.Session(profile_name = config['aws']['profile'])
kinesis_client = session.client('firehose', region_name = config['aws']['region'])

# Open connection to Orcle database, read data into dataframe and close connection
conn = cx_Oracle.connect(dsn=config['database']['dsn'],user=config['database']['user'],password=config['database']['pass'])
df = pd.read_sql_query("SELECT * FROM DMS_SAMPLE.SPORTING_EVENT_TICKET WHERE TICKETHOLDER_ID IS NOT NULL",conn)
conn.close()

# generate synthetic payload using the data frame
collection = generate_synthetic_payload(df)

print("Sending batches of 500 records...", end='',flush=True)
for batch in collection:
    send_batch_to_firehose(config['aws']['kinesis_stream'], batch)
    print('.', end = '',flush=True)


print('Done')
