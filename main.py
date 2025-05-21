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
import time
import pandas as pd
import threading

def process_buffer(channel: sock.CBChannel):
    '''Converts the buffer to pandas DF to process it'''
    while True:
        if channel.data_buffer:
            df = pd.DataFrame(channel.data_buffer)

            # run data_processor functions for df manipulation
            df = dp.clean(df)

        time.sleep(5) # runs every 5 seconds

def main():
    # ETH-USD channel
    eth_channel = sock.CBChannel('ETH-USD', channel='ticker')

    # BTC-USD channel
    btc_channel = sock.CBChannel('BTC-USD', channel='ticker')

    # launch both channels in threads
    eth_channel.start(threaded=True)
    btc_channel.start(threaded=True)

    # convert to DataFrame pandas processing
    threading.Thread(target=process_buffer, args=(eth_channel,), daemon=True).start()
    threading.Thread(target=process_buffer, args=(btc_channel,), daemon=True).start()

    # try-except while running
    try:
        while True:
            time.sleep(1)
    except:
        eth_channel.stop()
        btc_channel.stop()

if __name__=='__main__':
    main()