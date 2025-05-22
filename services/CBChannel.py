import json
import time
import threading
import pandas as pd
import util.data_processor as data_processor
from websocket import WebSocketApp # type: ignore


class CBChannel:
    """
    A WebSocket client for Coinbase Advanced Trade channels.

    Attributes:
        product (str): The trading pair, e.g., 'ETH-USD'
        channel (str): The channel type (default = 'ticker')
    """
    def __init__(self, product: str, channel: str = 'ticker'):
        self.product = product
        self.channel = channel
        self.ws_url = "wss://advanced-trade-ws.coinbase.com"

        self.ws = None
        self.last_sequence = None
        self.data_buffer = []
        self.keep_running = True  # used for clean shutdown

    def _on_open(self, ws):
        print(f"[{self.product}] Connection opened.")
        subscribe_message = {
            "type": "subscribe",
            "product_ids": [self.product],
            "channel": self.channel
        }
        ws.send(json.dumps(subscribe_message))

    def _on_message(self, ws, message):
        '''Receives and handles WebSocket output from coinbase'''
        data = json.loads(message)
        self.data_buffer.append(data)

        # check for sequence gaps
        sequence = data.get("sequence_num")
        if sequence is not None:
            if self.last_sequence is not None and sequence > self.last_sequence + 1:
                print(f"[{self.product}] Gap detected! Last: {self.last_sequence}, Current: {sequence}")
                ws.close()
            self.last_sequence = sequence

        # process in batches
        if len(self.data_buffer) >= 25:
            df = pd.DataFrame(self.data_buffer)
            
            series = data_processor.get_data(df)

            self.data_buffer.clear()
            
            return series

    def _on_close(self, ws, close_message, codes):
        '''Handles when connection closed'''
        print(f"[{self.product}] Connection closed.")
        if self.keep_running:
            print(f"[{self.product}] Reconnecting in 5 seconds...")
            time.sleep(5)
            self._start_ws()

    def _on_error(self, ws, error):
        '''Handles and prints connection error'''
        print(f"[{self.product}] Error: {error}")

    def _start_ws(self):
        '''Initializes and starts the web socket app'''
        self.ws = WebSocketApp(
            self.ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_close=self._on_close,
            on_error=self._on_error
        )
        self.ws.run_forever()

    def start(self, threaded: bool = False):
        """Starts the WebSocket connection."""
        if threaded:
            thread = threading.Thread(target=self._start_ws, daemon=True)
            thread.start()
        else:
            self._start_ws

    def stop(self):
        """Cleanly stops the connection."""
        self.keep_running = False
        if self.ws:
            self.ws.close()
        print(f"[{self.product}] Stopped.")

    def unsubscribe(self):
        """Sends an unsubscribe message to the WebSocket."""
        if self.ws:
            unsubscribe_message = {
                "type": "unsubscribe",
                "product_ids": [self.product],
                "channel": self.channel
            }
            self.ws.send(json.dumps(unsubscribe_message))
            print(f"[{self.product}] Unsubscribed.")
