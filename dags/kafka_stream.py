from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import uuid
import json
import time
import requests
from kafka import KafkaProducer
import logging

# Function to fetch data from the API
def get_data():
    try:
        res = requests.get("https://randomuser.me/api/")
        res.raise_for_status()  # Will raise HTTPError for bad responses
        res = res.json()
        return res['results'][0]
    except Exception as e:
        logging.error(f"Failed to fetch data: {e}")
        return None

# Function to format the data
def format_data(res):
    data = {}
    if res:
        location = res['location']
        data['id'] = str(uuid.uuid4())
        data['first_name'] = res['name']['first']
        data['last_name'] = res['name']['last']
        data['gender'] = res['gender']
        data['address'] = f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
        data['post_code'] = location['postcode']
        data['email'] = res['email']
        data['username'] = res['login']['username']
        data['dob'] = res['dob']['date']
        data['registered_date'] = res['registered']['date']
        data['phone'] = res['phone']
        data['picture'] = res['picture']['medium']
    return data

# Function to send data to Kafka
def stream_data():
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        acks='all',
        retries=5,
        max_block_ms=5000,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    curr_time = time.time()
    
    while time.time() < curr_time + 60:  # Stream data for 1 minute
        try:
            res = get_data()
            if res:
                formatted_data = format_data(res)
                logging.info(f"Sending data: {json.dumps(formatted_data)}")
                producer.send('users_created', formatted_data)
                producer.flush()
            time.sleep(5)
        except Exception as e:
            logging.error(f"Error in streaming data: {e}")
            continue

# Define the DAG
with DAG('kafka_stream', 
         default_args={'owner': 'airflow', 'start_date': datetime(2024, 11, 5)}, 
         schedule_interval='@daily',  # Set to run daily
         catchup=False) as dag:
    
    # Add the Kafka streaming task
    kafka_task = PythonOperator(
        task_id='stream_kafka_data',
        python_callable=stream_data
    )

    # Define task dependencies
    kafka_task  # This is the only task in this DAG
