Docs/
WebSocket/
Stocks/
Trades
Trades
WS
Real-Time: wss://socket.massive.com/stocks
Stream tick-level trade data for stock tickers via WebSocket. Each message delivers key trade details (price, size, exchange, conditions, and timestamps) as they occur, enabling users to track market activity, power live dashboards, and inform rapid decision-making.

Use Cases: Live monitoring, algorithmic trading, market analysis, data visualization.

Plan
Access
Stocks Basic
Stocks Starter
Stocks Developer
Stocks Advanced
Your plan
Stocks Business
Stocks Business + Expansion
Plan Access
Included in your Stocks plan

Plan
Recency
Stocks Basic
Not included
Stocks Starter
Not included
Stocks Developer
15-minute delayed
Stocks Advanced
Your plan
Real-time
Stocks Business
15-minute delayed
Stocks Business + Expansion
Real-time
Plan Recency
Real-time in your Stocks plan

Plan History
Not applicable to websockets
Parameters
Reset values
ticker
string
required
*
Specify a stock ticker or use * to subscribe to all stock tickers. You can also use a comma separated list to subscribe to multiple stock tickers. You can retrieve available stock tickers from our Stock Tickers API.
Response Attributes
ev
enum (T)
The event type.
sym
string
The ticker symbol for the given stock.
x
integer
The exchange ID. See Exchanges for Massive.com's mapping of exchange IDs.
i
string
The trade ID.
z
integer
The tape. (1 = NYSE, 2 = AMEX, 3 = Nasdaq).
p
number
The price.
s
integer
The trade size.
c
array (integer)
The trade conditions. See Conditions and Indicators for Massive.com's trade conditions glossary.
t
integer
The SIP timestamp in Unix MS.
q
integer
The sequence number represents the sequence in which message events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11).
trfi
integer
The ID for the Trade Reporting Facility where the trade took place.
trft
integer
The TRF (Trade Reporting Facility) Timestamp in Unix MS. This is the timestamp of when the trade reporting facility received this trade.
Code Examples

Shell

Python

Go

JavaScript

Kotlin


from massive import WebSocketClient
from massive.websocket.models import WebSocketMessage, Feed, Market
from typing import List

client = WebSocketClient(
	api_key="qRzMAhv3j6qfOvMNJ7LhO4qklEqRc0Rx",
	feed=Feed.RealTime,
	market=Market.Stocks
	)

# trades
client.subscribe("T.*")
# client.subscribe("T.*")  # all trades
# client.subscribe("T.TSLA") # single tickers
# client.subscribe("T.TSLA", "T.UBER") # multiple tickers

def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)

# print messages
client.run(handle_msg)
Subscription Request
WS
{"action":"subscribe", "params":"T.*"}

Choose an API key to update the code examples above
API key

m16
Response Object

Sample Response


{
  "ev": "T",
  "sym": "MSFT",
  "x": 4,
  "i": "12345",
  "z": 3,
  "p": 114.125,
  "s": 100,
  "c": [
    0,
    12
  ],
  "t": 1536036818784,
  "q": 3681328
}
