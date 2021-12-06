import requests

app_url = 'http://localhost:5000/'

def main():
    home(5)
    mine(4)
    return
        
def home(num_events=1):   
    for n in range(num_events): 
        endpoint = app_url 
        r = requests.get(endpoint)
​
def flag(num_events=1):    
    for n in range(num_events): 
        endpoint = app_url + 'flag?x=' + randrange(board_width) + '&y=' + randrange(board_height)
        r = requests.get(endpoint)
    
def check(x, y, num_events=1): 
    for n in range(num_events): 
        endpoint = app_url + 'check?x=' + randrange(board_width) + '&y=' + randrange(board_height)
        r = requests.get(endpoint)
        
def solution(num_events=1):     
    for n in range(num_events): 
        endpoint = app_url + 'solution'
        r = requests.get(endpoint)
        
if __name__ == '__main__':
    main()