# baDING
Tool to push automatic refresh updates into PowerBI baDING dashboard.

## Coinbase
So far the only fully functioning real-time data api is the coinbase socket.

BTC and ETH are pulled from their api, processed with util/data_processor, and pushed into PowerBI in main.py.

Any other coin can be captured by just giving it a thread in main and calling making it a CBChannel object.

## Tools
- requests
- json
- pandas
- threading
- websocket(coinbase api)

## Running App
1. Install all requirements.txt libraries
2. run "python main.py"

## baDING Dashboard
The datastream can be used in any Power BI workspace.

1. Create a workspace
2. Create a streaming dataset -> API
3. Set up all column names and value type
4. Run baDING in terminal 
5. Create reports/dashboard to see the data flow real-time