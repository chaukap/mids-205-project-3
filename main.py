import numpy as np
from numpy import random

from flask import Flask, request
app = Flask(__name__)

map = np.array(random.randint(100, size=(10000)) < 20).reshape((100, 100))

@app.route('/check', methods=['GET'])
def check():
    x = int(request.args.get('x'))
    y = int(request.args.get('y'))
    
    if(map[x][y]):
        return "Boom! Game Over."
    
    return "Safe!" 

@app.route('/flag', methods=['GET'])
def flag():
    x = int(request.args.get('x'))
    y = int(request.args.get('y'))
    
    if(map[x][y]):
        return "That's a bomb alright!"
    
    return "Nope! Game Over." 

@app.route('/solution', methods=['GET'])
def solution():
    return str(map)

if __name__ == '__main__':
    app.run()
