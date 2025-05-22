'''
A real-time data dashboard that uses the coinbase api to track cryptocurrency stocks.
Script will be fed into Power BI for the real-time dashboard display.

Tools:
- threading
- time
- pandas
'''
import services.CBChannel as sock
import util.data_processor as dp
from services.YahooMarkets import YahooMarkets
import time
import pandas as pd
import threading
import requests
import json

def process_buffer(channel: sock.CBChannel=None, yahoo: YahooMarkets=None):
    '''Converts the buffer to pandas DF to process it, pushes to power BI'''
    power_bi_url = "https://api.powerbi.com/beta/1ea2b65f-2f5e-440e-b025-dfdfafd8e097/datasets/b68a698d-3513-4fed-b351-4f4aebfe0eb8/rows?experience=power-bi&key=xlyKUMwPhw7MlAKxgvQIVGzfaZr65OCT3EWGjCytomKNbJBbRceslGWhINmpS9cPzsBbO%2B4zuoBZYi6g%2BOuhXA%3D%3D"

    while True:
        if channel and channel.data_buffer:
            try:
                cb_df = pd.DataFrame(channel.data_buffer)

                # run data_processor functions for df manipulation
                event_series = dp.get_data(cb_df)

                # construct return payload for POST
                payload = [{
                    "timestamp": pd.Timestamp.utcnow().isoformat()
                }]
                for key, value in event_series.items():
                    payload[0][key] = value

                headers = {"Content-Type": "application/json"}
                response = requests.post(power_bi_url, headers=headers, data=json.dumps(payload))

                print(json.dumps(payload, indent=2))

                if response.status_code != 200:
                    print(f'Power BI push failed: {response.status_code}, {response.text}')
            except Exception as e:
                print(f'Error processing buffer: {e}')

        if yahoo is not None:
            if len(yahoo.data) > 0:
                yahoo_df = pd.DataFrame(yahoo.data)
            else:
                print("Yahoo data not filled.")

        time.sleep(5) # runs every 5 seconds

def main():
    # ETH-USD channel
    eth_channel = sock.CBChannel('ETH-USD', channel='ticker')

    # BTC-USD channel
    btc_channel = sock.CBChannel('BTC-USD', channel='ticker')

    # launch both channels in threads
    eth_channel.start(threaded=True)
    btc_channel.start(threaded=True)

    # yahoo connection
    #yahoo_obj = YahooMarkets()

    # launch yahoo in thread
    #yahoo_obj.start(True)

    # convert to DataFrame pandas processing
    threading.Thread(target=process_buffer, args=(eth_channel,), daemon=True).start() # eth
    threading.Thread(target=process_buffer, args=(btc_channel,), daemon=True).start() # btc
    #threading.Thread(target=process_buffer, kwargs={"yahoo": yahoo_obj}, daemon=True).start() # yahoo

    # try-except while running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        eth_channel.stop()
        btc_channel.stop()
    
if __name__=='__main__':
    main()