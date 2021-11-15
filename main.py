import numpy as np
from numpy import random

from flask import Flask, request
app = Flask(__name__)

board = np.random.binomial(1, .2, size=10000).reshape(100,100)


####EVENTS:
#hit_mine_event
#uncover_safe_space_event
#true_positive_event (flag where there is a mine)
#false_positive_event (flag where there is no mine)


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())

@app.route('/check', methods=['GET'])
def check():
    x = int(request.args.get('x'))
    y = int(request.args.get('y'))
    hit_mine_event = {'event_type':'hit_mine'}
    uncover_safe_space_event = {'event_type':'uncover_safe_space'}

    if(board[x][y]):
        log_to_kafka('events', hit_mine_event)
        return "Boom! Game Over."

    log_to_kafka('events', uncover_safe_space_event)
    return "Safe!"

@app.route('/flag', methods=['GET'])
def flag():
    x = int(request.args.get('x'))
    y = int(request.args.get('y'))

    if(board[x][y]):
        return "That's a bomb alright!"

    return "Nope! Game Over." #should an incorrect flag end the game?

@app.route('/solution', methods=['GET'])
def solution():
    return str(board)

if __name__ == '__main__':
    app.run()
