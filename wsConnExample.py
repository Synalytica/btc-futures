from time import sleep
from binance.client import Client
from binance.websockets import BinanceSocketManager
from twisted.internet import reactor

client = Client()

# function processing the stream messages
def process_message(msg):
    print("message type: {}".format(msg['data']['e']))
    print(msg['data'])

# set a timeout of 60 seconds
bm = BinanceSocketManager(client, user_timeout=60)

# start any sockets here, i.e a futures mark price
conn_key = bm.start_symbol_mark_price_socket('BTCUSDT', process_message)
# then start the socket manager
bm.start()
#main thread to wait for 60 secs
sleep(60)
#this stops connection
bm.stop_socket(conn_key)
#this closes the socket manager but code wont exit
bm.close()

# when you need to exit
reactor.stop()