import yliveticker
import threading

class YahooMarkets:
    '''
    Launches yahoo market ticker.

    Attributes:
    - data(dict): stores the data received from yahoo
    '''
    def __init__(self):
        self.data = {}
    
    def on_new_msg(self, ws, msg):
        self.data = msg

    def launch(self, stocks: list=[]):
        tickers = ["BTC=X", "^GSPC", "^DJI", "^IXIC", "^RUT", "CL=F", "GC=F", "SI=F", "EURUSD=X", "^TNX", "^VIX", "GBPUSD=X", "JPY=X", "BTC-USD", "^CMC200", "^FTSE", "^N225"]

        if len(stocks) > 0:
            for stock in stocks:
                if stock not in tickers:
                    tickers.append(stock)
        
        yliveticker.YLiveTicker(on_ticker=self.on_new_msg, ticker_names=tickers)

    def start(self, threaded: bool = False):
        """Starts the Yahoo connection."""
        if threaded:
            thread = threading.Thread(target=self.launch, daemon=True)
            thread.start()
        else:
            self.launch
