Docs/
WebSocket/
Stocks/
Quotes
Quotes
WS
Real-Time: wss://socket.massive.com/stocks
Stream NBBO (National Best Bid and Offer) quote data for stock tickers via WebSocket. Each message provides the current best bid/ask prices, sizes, and related metadata as they update, allowing users to monitor evolving market conditions, inform trading decisions, and maintain responsive, data-driven applications.

Use Cases: Live monitoring, market analysis, trading decision support, dynamic interface updates.

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
Not included
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
enum (Q)
The event type.
sym
string
The ticker symbol for the given stock.
bx
integer
The bid exchange ID.
bp
number
The bid price.
bs
integer
The bid size. This represents the number of round lot orders at the given bid price. The normal round lot size is 100 shares. A bid size of 2 means there are 200 shares for purchase at the given bid price.
ax
integer
The ask exchange ID.
ap
number
The ask price.
as
integer
The ask size. This represents the number of round lot orders at the given ask price. The normal round lot size is 100 shares. An ask size of 2 means there are 200 shares available to purchase at the given ask price.
c
integer
The condition.
i
array (integer)
The indicators. For more information, see our glossary of Conditions and Indicators.
t
integer
The SIP timestamp in Unix MS.
q
integer
The sequence number represents the sequence in which quote events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11). Values reset after each trading session/day.
z
integer
The tape. (1 = NYSE, 2 = AMEX, 3 = Nasdaq).
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

# quotes
client.subscribe("Q.*")
# client.subscribe("Q.*")  # all quotes
# client.subscribe("Q.TSLA", "Q.UBER") # single ticker
# client.subscribe("Q.TSLA", "Q.UBER") # multiple tickers

def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)

# print messages
client.run(handle_msg)
Subscription Request
WS
{"action":"subscribe", "params":"Q.*"}

Choose an API key to update the code examples above
API key

m16
Response Object

Sample Response


{
  "ev": "Q",
  "sym": "MSFT",
  "bx": 4,
  "bp": 114.125,
  "bs": 100,
  "ax": 7,
  "ap": 114.128,
  "as": 160,
  "c": 0,
  "i": [
    604
  ],
  "t": 1536036818784,
  "q": 50385480,
  "z": 3
}
