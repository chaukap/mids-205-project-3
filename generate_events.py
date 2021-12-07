import requests
from random import randrange

app_url = 'http://localhost:5000/'
board_width = 100
board_height = 100

def main():
    """main
    """
    for i in range(0, 5):
        user = home()
        check(user, 8)
        flag(user, 8)
        solution(user, 8)

def home():
    endpoint = app_url
    response = requests.get(endpoint)
    return response.content

def flag(session_id, num_events=1):    
    for n in range(num_events): 
        endpoint = app_url + 'flag?x=' + str(randrange(board_width)) + '&y=' + str(randrange(board_height)) + '&session_id=' + str(session_id)
        r = requests.get(endpoint)
    
def check(session_id, num_events=1): 
    for n in range(num_events): 
        endpoint = app_url + 'check?x=' + str(randrange(board_width)) + '&y=' + str(randrange(board_height)) + '&session_id=' + str(session_id)
        r = requests.get(endpoint)
        
def solution(session_id, num_events=1):     
    for n in range(num_events): 
        endpoint = app_url + 'solution?session_id=' + str(session_id)
        r = requests.get(endpoint)

if __name__ == "__main__":
    main()