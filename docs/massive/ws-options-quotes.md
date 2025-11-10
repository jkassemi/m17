Docs/
WebSocket/
Options/
Quotes
Quotes
WS
Real-Time: wss://socket.massive.com/options
Stream quote data for specified options contracts via WebSocket. Each message delivers current best bid/ask prices, sizes, and associated metadata as they update, enabling users to monitor dynamic market conditions and inform trading decisions. Due to the high bandwidth and message rates associated with options quotes, users can subscribe to a maximum of 1,000 option contracts per connection.

Use Cases: Live monitoring, market analysis, trading decision support, dynamic interface updates.

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
Not included
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
Specify an option contract. You're only allowed to subscribe to 1,000 option contracts per connection. You can also use a comma separated list to subscribe to multiple option contracts. You can retrieve active options contracts from our Options Contracts API.
Response Attributes
ev
enum (Q)
The event type.
sym
string
The ticker symbol for the given option contract.
bx
integer
The bid exchange ID. See Exchanges for Massive.com's mapping of exchange IDs.
ax
integer
The ask exchange ID. See Exchanges for Massive.com's mapping of exchange IDs.
bp
number
The bid price.
ap
number
The ask price.
bs
integer
The bid size.
as
integer
The ask size.
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

# quotes (1,000 option contracts per connection)
client.subscribe("Q.O:SPY251219C00650000")
# client.subscribe("Q.O:SPY241220P00720000", "Q.O:SPY251219C00650000")

def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)

# print messages
client.run(handle_msg)
Subscription Request
WS
{"action":"subscribe", "params":"Q.O:SPY251219C00650000"}

Choose an API key to update the code examples above
API key

m16
Response Object

Sample Response


{
  "ev": "Q",
  "sym": "O:SPY241220P00720000",
  "bx": 302,
  "ax": 302,
  "bp": 9.71,
  "ap": 9.81,
  "bs": 17,
  "as": 24,
  "t": 1644506128351,
  "q": 844090872
}
