Docs/
REST API/
Options/
Snapshots/
Option Chain Snapshot
Option Chain Snapshot
GET
/v3/snapshot/options/{underlyingAsset}
Retrieve a comprehensive snapshot of all options contracts associated with a specified underlying ticker. This endpoint consolidates key metrics for each contract, including pricing details, greeks (delta, gamma, theta, vega), implied volatility, quotes, trades, and open interest. Users also gain insights into the underlying asset’s current price and break-even calculations. By examining the full options chain in a single request, traders and analysts can evaluate market conditions, compare contract attributes, and refine their strategies.

Use Cases: Market overview, strategy comparison, research and modeling, portfolio refinement.

Plan
Access
Options Basic
Options Starter
Options Developer
Options Advanced
Your plan
Options Business
Plan Access
Included in your Options plan

Plan
Recency
Options Basic
Not included
Options Starter
15-minute delayed
Options Developer
15-minute delayed
Options Advanced
Your plan
Real-time
Options Business
Real-time
Plan Recency
Real-time in your Options plan

Plan History
Not applicable to this endpoint
Path Parameters
Reset values
underlyingAsset
string
required
A
The underlying ticker symbol of the option contract.
Query Parameters
strike_price
number
Query by strike price of a contract.

Show filter modifiers
expiration_date
string
Query by contract expiration with date format YYYY-MM-DD.

Show filter modifiers
contract_type
enum (string)

Select
Query by the type of contract.
order
enum (string)

asc
Order results based on the `sort` field.
limit
integer
10
Limit the number of results returned, default is 10 and max is 250.
sort
enum (string)

ticker
Sort field used for ordering.
Response Attributes
next_url
string
optional
If present, this value can be used to fetch the next page of data.
request_id
string
A request id assigned by the server.
results
array (object)
optional
An array of results containing the requested data.

Hide child attributes
break_even_price
number
The price of the underlying asset for the contract to break even. For a call, this value is (strike price + premium paid). For a put, this value is (strike price - premium paid).
Plan
Response attribute
Options Basic
Options Starter
Options Developer
Options Advanced
Options Business
Options Business + Expansion
Access included in select Options plans

day
object
The most recent daily bar for this contract.

Show child attributes
details
object
The details for this contract.

Show child attributes
fmv
number
optional
Fair Market Value is only available on Business plans. It is our proprietary algorithm to generate a real-time, accurate, fair market value of a tradable security. For more information, contact us.
Plan
Response attribute
Options Basic
Options Starter
Options Developer
Options Advanced
Options Business
Options Business + Expansion
Access included in select Options plans

fmv_last_updated
integer
optional
If Fair Market Value (FMV) is available, this field is the nanosecond timestamp of the last FMV calculation.
Plan
Response attribute
Options Basic
Options Starter
Options Developer
Options Advanced
Options Business
Options Business + Expansion
Access included in select Options plans

greeks
object
optional
The greeks for this contract. There are certain circumstances where greeks will not be returned, such as options contracts that are deep in the money. See this article for more information.

Show child attributes
implied_volatility
number
optional
The market's forecast for the volatility of the underlying asset, based on this option's current price.
last_quote
object
The most recent quote for this contract. This is only returned if your current plan includes quotes.
Plan
Response attribute
Options Basic
Options Starter
Options Developer
Options Advanced
Options Business
Options Business + Expansion
Access included in select Options plans


Show child attributes
last_trade
object
optional
The most recent trade for this contract. This is only returned if your current plan includes trades.
Plan
Response attribute
Options Basic
Options Starter
Options Developer
Options Advanced
Options Business
Options Business + Expansion
Access included in select Options plans


Show child attributes
open_interest
number
The quantity of this contract held at the end of the last trading day.
underlying_asset
object
Information on the underlying stock for this options contract. The market data returned depends on your current stocks plan.

Show child attributes
status
string
The status of this request's response.
Code Examples

Shell

Python

Go

JavaScript

Kotlin


from massive import RESTClient

client = RESTClient("qRzMAhv3j6qfOvMNJ7LhO4qklEqRc0Rx")

options_chain = []
for o in client.list_snapshot_options_chain(
    "A",
    params={
        "order": "asc",
        "limit": 10,
        "sort": "ticker",
    },
):
    options_chain.append(o)

print(options_chain)
Query URL
GET
https://api.massive.com/v3/snapshot/options/A?order=asc&limit=10&sort=ticker&apiKey=qRzMAhv3j6qfOvMNJ7LhO4qklEqRc0Rx

Click "Run Query" to view the API response below
API key

m16

Run Query
Scroll to see updated query response
Response Object

Sample Response

Query Response


{
  "request_id": "6a7e466379af0a71039d60cc78e72282",
  "results": [
    {
      "break_even_price": 151.2,
      "day": {
        "change": 4.5,
        "change_percent": 6.76,
        "close": 120.73,
        "high": 120.81,
        "last_updated": 1605195918507251700,
        "low": 118.9,
        "open": 119.32,
        "previous_close": 119.12,
        "volume": 868,
        "vwap": 119.31
      },
      "details": {
        "contract_type": "call",
        "exercise_style": "american",
        "expiration_date": "2022-01-21",
        "shares_per_contract": 100,
        "strike_price": 150,
        "ticker": "O:AAPL211022C000150000"
      },
      "fmv": 0.05,
      "fmv_last_updated": 1605195918508251600,
      "greeks": {
        "delta": 1,
        "gamma": 0,
        "theta": 0.00229,
        "vega": 0
      },
      "implied_volatility": 5,
      "last_quote": {
        "ask": 120.3,
        "ask_size": 4,
        "bid": 120.28,
        "bid_size": 8,
        "last_updated": 1605195918507251700,
        "midpoint": 120.29,
        "timeframe": "REAL-TIME"
      },
      "last_trade": {
        "conditions": [
          209
        ],
        "exchange": 316,
        "price": 0.05,
        "sip_timestamp": 1675280958783136800,
        "size": 2,
        "timeframe": "REAL-TIME"
      },
      "open_interest": 1543,
      "underlying_asset": {
        "change_to_break_even": 4.2,
        "last_updated": 1605195918507251700,
        "price": 147,
        "ticker": "AAPL",
        "timeframe": "DELAYED"
      }
    }
  ],
  "status": "OK"
}
