import numpy as np
from numpy import random
from kafka import KafkaProducer
from flask import Flask, request, render_template

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')

board = np.random.binomial(1, .2, size=10000).reshape(100,100)



####EVENTS:
hit_mine_event = {'event_type':'hit_mine'}
hit_safe_space_event = {'event_type':'uncover_safe_space'}
correct_flag_event = {'event_type':'correct_flag'}
incorrect_flag_event = {'event_type':'incorrect_flag'}


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
    return "Safe!"
    
    
@app.route('/check', methods=['GET'])
def check():
    x = 3 #int(request.args.get('x'))
    y = 4 #int(request.args.get('y'))
    hit_mine_event = {'event_type':'hit_mine'}
    hit_safe_space_event = {'event_type':'uncover_safe_space'}

    if(board[x][y]):
        log_to_kafka('events', hit_mine_event)
        return "Boom! Game Over."

    log_to_kafka('events', hit_safe_space_event)
    return "Safe!"




@app.route('/flag', methods=['GET'])
def flag():
    x = 16 #int(request.args.get('x'))
    y = 24 #int(request.args.get('y'))
    correct_flag_event = {'event_type':'correct_flag'}
    incorrect_flag_event= {'event_type':'incorrect_flag'}
    
    if(board[x][y]):
        log_to_kafka('events', correct_flag_event)
        return "That's a bomb alright!"

    log_to_kafka('events', incorrect_flag_event)
    return "Nope! Game Over." #should an incorrect flag end the game?



@app.route('/solution', methods=['GET'])
def solution():
    return str(board)

if __name__ == '__main__':
    app.run()
