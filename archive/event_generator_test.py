import requests

app_url = 'http://localhost:5000/'

def main():
    home(5)
    mine(4)
    return


def home(num_events = 1):
    #http://0.0.0.0:5000/
    for i in range(num_events):
        endpoint = app_url
        r = requests.get(endpoint)
    
def mine(num_events = 1):
    #http://0.0.0.0:5000/mine
    for i in range(num_events):
        endpoint = app_url +'mine'
        r = requests.get(endpoint)
        
if __name__ == '__main__':
    main()