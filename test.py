#simplified messages being written
from kafka import KafkaProducer
from flask import Flask, request, render_template

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')



def log_to_kafka(topic, event):
    producer.send(topic, json.dumps(event).encode())

# def log_to_kafka(topic, event):
#     event.update(request.headers)
#     producer.send(topic, json.dumps(event).encode())
#     return
    
@app.route('/')
def home():
    startup_event = {'event_type':'a_startup_event'}
    log_to_kafka('events', test_event)
    return 'Welcome to Minesweeper!'


@app.route('/test', methods=['GET'])
def test():
    test_event = {'event_type':'a_test_event'}
    log_to_kafka('events', test_event)
    return "The test function worked properly!"
    

if __name__ == '__main__':
    app.run()
