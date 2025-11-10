Treasury Yields
GET
/fed/v1/treasury-yields
Retrieve historical U.S. Treasury yield data for standard timeframes ranging from 1-month to 30-years, with daily historical records back to 1962. This endpoint lets you query by date or date range to see how interest rates have changed over time. Each data point reflects the market yield for Treasury securities of a specific maturity, helping users understand short- and long-term rate movements.

Use Cases: Charting rate trends, comparing short vs. long-term yields, economic research.

Plan
Access
All Stocks plans
All Options plans
All Indices plans
All Currencies plans
Plan Access
Included in all plans

Plan
Recency
All Stocks plans
Updated daily
All Options plans
Updated daily
All Indices plans
Updated daily
All Currencies plans
Updated daily
Plan Recency
Updated daily

Plan
History
All Stocks plans
All history
All Options plans
All history
All Indices plans
All history
All Currencies plans
All history
Plan History
All history (records date back to January 2, 1962)

Query Parameters
Reset values
date
string
Calendar date of the yield observation (YYYY-MM-DD).

Show filter modifiers
limit
integer
100
Limit the maximum number of results returned. Defaults to '100' if not specified. The maximum allowed limit is '50000'.
sort
string
date.asc
A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'date' if not specified. The sort order defaults to 'asc' if not specified.
Response Attributes
next_url
string
optional
If present, this value can be used to fetch the next page.
request_id
string
A request id assigned by the server.
results
array (object)
The results for this request.

Hide child attributes
date
string
optional
Calendar date of the yield observation (YYYY-MM-DD).
yield_10_year
number
optional
Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity, Quoted on an Investment Basis
yield_1_month
number
optional
Market Yield on U.S. Treasury Securities at 1-Month Constant Maturity, Quoted on an Investment Basis
yield_1_year
number
optional
Market Yield on U.S. Treasury Securities at 1-Year Constant Maturity, Quoted on an Investment Basis
yield_20_year
number
optional
Market Yield on U.S. Treasury Securities at 20-Year Constant Maturity, Quoted on an Investment Basis
yield_2_year
number
optional
Market Yield on U.S. Treasury Securities at 2-Year Constant Maturity, Quoted on an Investment Basis
yield_30_year
number
optional
Market Yield on U.S. Treasury Securities at 30-Year Constant Maturity, Quoted on an Investment Basis
yield_3_month
number
optional
Market Yield on U.S. Treasury Securities at 3-Month Constant Maturity, Quoted on an Investment Basis
yield_3_year
number
optional
Market Yield on U.S. Treasury Securities at 3-Year Constant Maturity, Quoted on an Investment Basis
yield_5_year
number
optional
Market Yield on U.S. Treasury Securities at 5-Year Constant Maturity, Quoted on an Investment Basis
yield_6_month
number
optional
Market Yield on U.S. Treasury Securities at 6-Month Constant Maturity, Quoted on an Investment Basis
yield_7_year
number
optional
Market Yield on U.S. Treasury Securities at 7-Year Constant Maturity, Quoted on an Investment Basis
status
enum (OK)
The status of this request's response.
Code Examples

Shell

Python

Go

JavaScript

Kotlin


from massive import RESTClient

client = RESTClient("qRzMAhv3j6qfOvMNJ7LhO4qklEqRc0Rx")

yields = []
for date in client.list_treasury_yields(
	limit=100,
	sort="date.asc",
	):
    yields.append(date)

print(yields)
Query URL
GET
https://api.massive.com/fed/v1/treasury-yields?limit=100&sort=date.asc&apiKey=qRzMAhv3j6qfOvMNJ7LhO4qklEqRc0Rx

Click "Run Query" to view the API response below
API key

m16

Run Query
Scroll to see updated query response
Response Object

Sample Response

Query Response


{
  "count": 1,
  "request_id": 1,
  "results": [
    {
      "date": "1962-01-02",
      "yield_10_year": 4.06,
      "yield_1_year": 3.22,
      "yield_5_year": 3.88
    }
  ],
  "status": "OK"
}
