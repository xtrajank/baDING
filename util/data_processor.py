'''
Process pandas data from sockets for displaying.

Tools:
- Pandas
'''
import pandas as pd

def clean(df:pd.DataFrame)-> pd.DataFrame:
    df = pd.DataFrame(columns=["time", "product_id", "price", "volume"])
    df.dropna()

    return df

def get_events(df:pd.DataFrame)-> pd.DataFrame:
    return df["events"]

