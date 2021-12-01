# import numpy as np
import uuid
# from numpy import random
# from kafka import KafkaProducer
from flask import Flask, request, render_template, redirect, url_for
from datetime import datetime
import redis
import random

redis_client = redis.Redis(host='redis', port='6379')

app = Flask(__name__)
# producer = KafkaProducer(bootstrap_servers='kafka:29092')

board_size = 100
# board = np.random.binomial(1, .2, size=10000).reshape(100,100)

####EVENTS:
hit_mine_event = {'event_type':'hit_mine'}
hit_safe_space_event = {'event_type':'uncover_safe_space'}
correct_flag_event = {'event_type':'correct_flag'}
incorrect_flag_event = {'event_type':'incorrect_flag'}


def log_to_kafka(topic, event):
    print(str(event))
    # producer.send(topic, json.dumps(event).encode())

# def log_to_kafka(topic, event):
#     event.update(request.headers)
#     producer.send(topic, json.dumps(event).encode())
#     return

def get_mines(session_id):
    mines = redis_client.get(session_id)
    return mines.split(" ")
    
    
@app.route('/')
def home():
    session_id = str(uuid.uuid4())
    max_index = (board_size * board_size) - 1
    mines = [str(random.randint(0, max_index)) for i in range(0,int((board_size * board_size) * 0.2))]
    
    redis_client.set(session_id, " ".join(mines))
    
    log_to_kafka('events', {
        'event_type':'a_startup_event',
        'session_id': session_id,
        'datetime': str(datetime.utcnow())
    })
    
    return str(session_id)  


@app.route('/check', methods=['GET'])
def check():
    x = int(request.args.get('x'))
    y = int(request.args.get('y'))
    session_id = request.args.get('session_id')
    
    if session_id == None:
        return redirect(url_for('home'))
    
    mines = get_mines(session_id)
    flat_location = str(x * board_size + y)

    if(flat_location in mines):
        log_to_kafka('events', {
            'event_type': 'hit_mine',
            'x_coord': x,
            'y_coord': y,
            'session_id': session_id,
            'datetime': str(datetime.utcnow())
        })
        return "Boom! Game Over."

    log_to_kafka('events', {
        'event_type':'uncover_safe_space',
        'x_coord': x,
        'y_coord': y,
        'session_id': session_id,
        'datetime': str(datetime.utcnow())
    })
    return "Safe!"


@app.route('/flag', methods=['GET'])
def flag():
    x = int(request.args.get('x')) if int(request.args.get('x')) is not None else -1
    y = int(request.args.get('y')) if int(request.args.get('y')) is not None else -1
    session_id = request.args.get('session_id')
    
    if x == -1 or y == -1:
        return "Invalid coordinates."
    
    if session_id == None:
        return redirect(url_for('home'))
    
    mines = get_mines(session_id)
    flat_location = str(x * board_size + y)

    if(flat_location in mines):
        log_to_kafka('events', {
            'event_type':'correct_flag',
            'x_coord': x,
            'y_coord': y,
            'session_id': session_id,
            'datetime': str(datetime.utcnow())
        })
        return "That's a bomb alright!"

    log_to_kafka('events', {
        'event_type':'incorrect_flag',
        'x_coord': x,
        'y_coord': y,
        'session_id': session_id,
        'datetime': str(datetime.utcnow())
    })
    
    return "Nope! Game Over."

@app.route('/solution', methods=['GET'])
def solution():
    session_id = request.args.get('session_id')
    
    if session_id == None:
        return redirect(url_for('home'))
    
    board = [[0 for z in range(0, board_size)] for i in range(0, board_size)]
    
    mines = get_mines(session_id)
    
    for mine in mines:
        x = int(int(mine) / board_size)
        y = int(int(mine) % board_size)
        board[x][y] = 1
    
    log_to_kafka('events', {
        'event_type':'solution',
        'session_id': session_id,
        'datetime': str(datetime.utcnow())
    })
    
    return str(board)

if __name__ == '__main__':
    app.run()
