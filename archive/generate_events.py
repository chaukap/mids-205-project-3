import requests
from random import randrange

app_url = 'http://localhost:5000/'
board_width = 100
board_height = 100

def main():
    """main
    """
    session_id = home()
    check(session_id, 5)
    flag(session_id, 5)
    solution(session_id, 1)

def home():
    endpoint = app_url
    response = requests.get(endpoint)
    return response.content

def flag(session_id, num_events=1):    
    for n in range(num_events): 
        endpoint = f'{app_url}flag?x={randrange(board_width)}&y={randrange(board_height)}&session_id={str(session_id)}'
        r = requests.get(endpoint)
    
def check(session_id, num_events=1): 
    for n in range(num_events): 
        endpoint = f'{app_url}check?x={randrange(board_width)}&y={randrange(board_height)}&session_id={str(session_id)}'
        r = requests.get(endpoint)
        
def solution(session_id, num_events=1):     
    for n in range(num_events): 
        endpoint = f'{app_url}solution?session_id={session_id}'
        r = requests.get(endpoint)

if __name__ == "__main__":
    main()