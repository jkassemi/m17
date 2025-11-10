Trades
S3
us_stocks_sip/trades_v1
Tick level nanosecond timestamped trade data from all major U.S. exchanges and darkpools including NYSE, Nasdaq, Cboe, Finra, and more, made available as a daily downloadable S3 file.
Plan
Access
Stocks Basic
Stocks Starter
Stocks Developer
Stocks Advanced
Your plan
Stocks Business
Plan Access
Included in your Stocks plan

Plan
Recency
Stocks Basic
Not included
Stocks Starter
Not included
Stocks Developer
End-of-day
Stocks Advanced
Your plan
End-of-day
Stocks Business
End-of-day
Plan Recency
Included in your Stocks plan (updated at 11a ET to include the previous day)

Plan
History
Stocks Basic
Not included
Stocks Starter
Not included
Stocks Developer
10 years
Stocks Advanced
Your plan
All history
Stocks Business
All history
Plan History
All history in your Stocks plan (records date back to September 10, 2003)

File Browser
Documentation
Documentation
Sample file
ticker	conditions	correction	exchange	id	participant_timestamp	price	sequence_number	sip_timestamp	size	tape	trf_id	trf_timestamp
MSFT	12,37		11	1	1679990400018025984	276.16	1550	1679990400018381329	55	3		0
MSFT	12		8	1	1679990400000110000	276.75	1556	1679990400034839796	100	3		0
MSFT	12,37		8	2	1679990400000110000	276.75	1557	1679990400034849885	50	3		0
MSFT	12,37		8	3	1679990400000110000	276.7	1558	1679990400034864884	5	3		0
MSFT	12,37		8	4	1679990400000110000	276.7	1559	1679990400034869044	10	3		0
MSFT	12,37		8	5	1679990400000110000	276.69	1560	1679990400034875659	1	3		0
MSFT	12,37		8	6	1679990400000110000	276.69	1561	1679990400034882414	3	3		0
MSFT	12,37		8	7	1679990400000110000	276.69	1562	1679990400034886593	40	3		0
MSFT	12,37		8	8	1679990400000110000	276.64	1563	1679990400034893318	40	3		0
MSFT	12,37		8	9	1679990400067336000	276.52	1582	1679990400067544813	27	3		0
Columns
conditions
integer
A list of condition codes. If there are multiple conditions, they will appear in quotations, separated by commas. See Trade Conditions for Massive.com’s definitions of each condition.
correction
integer
The trade correction indicator.
exchange
integer
The exchange ID. See Exchanges for Massive.com's mapping of exchange IDs.
id
string
The Trade ID which uniquely identifies a trade. These are unique per combination of ticker, exchange, and TRF. For example: A trade for AAPL executed on NYSE and a trade for AAPL executed on NASDAQ could potentially have the same Trade ID.
participant_timestamp
integer
The nanosecond accuracy Participant/Exchange Unix Timestamp. This is the timestamp of when the trade was actually generated at the exchange.
price
number
The price of the trade. This is the actual dollar value per whole share of this trade. A trade of 100 shares with a price of $2.00 would be worth a total dollar value of $200.00.
sequence_number
integer
The sequence number represents the sequence in which trade events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11). Values reset after each trading session/day.
sip_timestamp
integer
The nanosecond accuracy SIP Unix Timestamp. This is the timestamp of when the SIP received this trade from the exchange which produced it.
size
number
The size of a trade (also known as volume).
tape
integer
There are 3 tapes which define which exchange the ticker is listed on. These are integers in our objects which represent the letter of the alphabet. Eg: 1 = A, 2 = B, 3 = C.
Tape A is NYSE listed securities
Tape B is NYSE ARCA / NYSE American
Tape C is NASDAQ
ticker
string
The exchange symbol that this item is traded under.
trf_id
integer
The ID for the Trade Reporting Facility where the trade took place.
trf_timestamp
integer
The nanosecond accuracy TRF (Trade Reporting Facility) Unix Timestamp. This is the timestamp of when the trade reporting facility received this trade.
