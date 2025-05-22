'''
Process pandas data from sockets for displaying.

Tools:
- Pandas
'''
import pandas as pd

def clean_cb(df:pd.DataFrame)-> pd.DataFrame:
    df = pd.DataFrame(columns=["time", "product_id", "price", "volume"])
    df.dropna()

    return df

def get_events(df:pd.DataFrame)-> pd.DataFrame:
    return df["events"]

def try_parse_float(x):
    try:
        return float(x)
    except (ValueError, TypeError):
        return x 

def get_data(df:pd.DataFrame)-> pd.Series:
    events = df["events"]

    event = pd.Series(events[0][0]['tickers'][0])

    event = event.drop('type', errors='ignore')

    event = event.apply(try_parse_float)

    print(event)
    return event
