#no kafka messages being written

from flask import Flask

app = Flask(__name__)

start = 1

@app.route('/')
def hello():
    return 'THIS IS THE HOME PAGE!'


@app.route('/create_file')
def create_a_file():
    with open('this_is_a_test.txt', 'w') as f:
        f.write(f'Creating file and writing first line!\n')
        f.close()
    return 'YOU JUST CREATED A TEXT FILE AND WROTE THE FIRST LINE'


@app.route('/add_to_file')
def add_a_line():

    global start

    start+=1

    with open('this_is_a_test.txt', 'a') as f:
        f.write(f'Adding a line!\n')
        f.close()


    return f'YOU JUST ADDED LINE {start}\n'


# @app.route('/test_page')
# def print()
