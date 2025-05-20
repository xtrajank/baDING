'''
A class to open specific Coinbase channels to distribute the connection's load
'''
from websocket import WebSocketApp
import json
import time

class CBChannel:
    def __init__(self, product, channel='heartbeats'):
        self.product = product
        self.ws_url = "wss://advanced-trade-ws.coinbase.com" # Coinbase WebSocket Market Data Endpoint
        self.last_sequence = None
        self.message_buffer = []
        self.channel = channel
        self.ws = None

    def launch(self):
        '''
        Launches the channel subscription
        '''
        # subscribe to specified channel
        subscribe_message = {
            "type": "subscribe",
            "product_ids": [self.product],
            "channel": self.channel
        }
        
        # sends the subscription to the socket
        def on_open(ws):
            print("Socket opened.")
            self.ws.send(json.dumps(subscribe_message))

        # handle incoming messages, checks for gaps in data flow
        def on_message(ws, message):
            data = json.loads(message)
            self.message_buffer.append(data)

            # handle message loads
            if len(self.message_buffer) >= 100:
                for message in self.message_buffer:
                    print(message)
                self.message_buffer.clear()

            # check for sequence gaps
            if "sequence_num" in data:
                curr_sequence = data["sequence_num"]
                if self.last_sequence is not None and curr_sequence > self.last_sequence + 1:
                    print(f'Gap detected. Resync required. Last: {self.last_sequence}, Current: {curr_sequence}')
                    on_close(self.ws)

                self.last_sequence = curr_sequence


        def on_close(ws):
            print("Connection closed. Reconnecting...")
            reconnect()

        def reconnect():
            time.sleep(5)
            self.ws.run_forever()

        # connects
        self.ws = WebSocketApp(self.ws_url, on_open=on_open, on_message=on_message, on_close=on_close)
        self.ws.run_forever()

    def unsubscribe(self):
        unsubscribe_message = {
            "type": "unsubscribe",
            "product_ids": [self.product],
            "channel": self.channel
        }
        if self.ws:
            self.ws(json.dumps(unsubscribe_message))
            print("Unsubscribed from", self.product)

