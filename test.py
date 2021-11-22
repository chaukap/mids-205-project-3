#simplified messages being written
import json
# import numpy as np
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')

def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())

@app.route('/')
def home():
    startup_event = {'event_type':'a_startup_event'}
    log_to_kafka('events', startup_event)
    return 'Welcome to Minesweeper!\n'


@app.route('/mine', methods=['GET'])
def mine():
    test_event = {'event_type':'a_test_event'}
    log_to_kafka('events', test_event)
    return "You clicked on a mine! Game over!\n"
    

# if __name__ == '__main__':
#     app.run()
