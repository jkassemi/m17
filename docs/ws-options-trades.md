Docs/
WebSocket/
Options/
Trades
Trades
WS
Real-Time: wss://socket.massive.com/options
Stream tick-level trade data for option contracts via WebSocket. Each message delivers key trade details (price, size, exchange, conditions, and timestamps) as they occur, enabling users to track market activity, power live dashboards, and inform rapid decision-making.

Use Cases: Live monitoring, algorithmic trading, market analysis, data visualization.

Plan
Access
Options Basic
Options Starter
Options Developer
Options Advanced
Your plan
Options Business
Options Business + Expansion
Plan Access
Included in your Options plan

Plan
Recency
Options Basic
Not included
Options Starter
Not included
Options Developer
15-minute delayed
Options Advanced
Your plan
Real-time
Options Business
Not included
Options Business + Expansion
Real-time
Plan Recency
Real-time in your Options plan

Plan History
Not applicable to websockets
Parameters
Reset values
ticker
string
required
O:SPY251219C00650000
Specify an option contract or use * to subscribe to all option contracts. You can also use a comma separated list to subscribe to multiple option contracts. You can retrieve active options contracts from our Options Contracts API.
Response Attributes
ev
enum (T)
The event type.
sym
string
The ticker symbol for the given option contract.
x
integer
The exchange ID. See Exchanges for Massive.com's mapping of exchange IDs.
p
number
The price.
s
integer
The trade size.
c
array (integer)
The trade conditions
t
integer
The Timestamp in Unix MS.
q
integer
The sequence number represents the sequence in which trade events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11).
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
	market=Market.Options
	)

# trades
client.subscribe("T.O:SPY251219C00650000")
# client.subscribe("T.*") # all trades
# client.subscribe("T.O:SPY241220P00720000", "T.O:SPY251219C00650000")

def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)

# print messages
client.run(handle_msg)
Subscription Request
WS
{"action":"subscribe", "params":"T.O:SPY251219C00650000"}

Choose an API key to update the code examples above
API key

m16
Response Object

Sample Response


{
  "ev": "T",
  "sym": "O:AMC210827C00037000",
  "x": 65,
  "p": 1.54,
  "s": 1,
  "c": [
    233
  ],
  "t": 1629820676333,
  "q": 651921857
}
