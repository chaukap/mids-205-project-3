import uuid
from flask import Flask, request, render_template, redirect, url_for
from datetime import datetime
from kafka import KafkaProducer
import redis
import random
import json

redis_client = redis.Redis(host='redis', port='6379')

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')

board_size = 100

####EVENTS:
startup = {'event_type':'a_startup_event'}
check = {'event_type':'check'}
flag = {'event_type':'flag'}
solution_event = {'event_type':'solution'}

def log_to_kafka(topic, event):
    producer.send(topic, json.dumps(event).encode())

def get_mines(session_id):
    mines = redis_client.get(session_id)
    return mines.split(" ")

def neighboring_bombs(i,j,session_id):
    total = 0
    start_row = i-1
    start_column = j-1
    board = [[0 for z in range(0, board_size)] for i in range(0, board_size)]

    mines = get_mines(session_id)
    for mine in mines:
        x = int(int(mine) / board_size)
        y = int(int(mine) % board_size)
        board[x][y] = 1
        
    if i == 0:
        start_row = i
    if j == 0:
        start_column = j    
    for row in range(start_row, i+2):
        for column in range(start_column, j+2):
            try:
                total += board[row][column]
            except:
                pass

    return (total)


@app.route('/')
def home():
    session_id = str(uuid.uuid4())
    max_index = (board_size * board_size) - 1
    mines = [str(random.randint(0, max_index)) for i in range(0,int((board_size * board_size) * 0.2))]
    redis_client.set(session_id, " ".join(mines))

    log_to_kafka('events', {
        'event_type':'a_startup_event',
        'session_id': session_id
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

    if flat_location in mines:
        log_to_kafka('events', {
            'event_type': 'check',
            'outcome':'hit_mine',
            'neighboring_bombs': neighboring_bombs(x,y,session_id),
            'x_coord': x,
            'y_coord': y,
            'session_id': session_id
        })
        return "Boom! Game Over."

    log_to_kafka('events', {
        'event_type':'check',
        'outcome':'safe',
        'neighboring_bombs': neighboring_bombs(x,y,session_id),
        'x_coord': x,
        'y_coord': y,
        'session_id': session_id
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

    if flat_location in mines:
        log_to_kafka('events', {
            'event_type':'flag',
            'outcome':'correct',
            'x_coord': x,
            'y_coord': y,
            'session_id': session_id
        })
        return "That's a bomb alright!"

    log_to_kafka('events', {
        'event_type':'flag',
        'outcome':'incorrect',
        'x_coord': x,
        'y_coord': y,
        'session_id': session_id
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
        'session_id': session_id
    })

    return str(board)

if __name__ == '__main__':
    app.run()
