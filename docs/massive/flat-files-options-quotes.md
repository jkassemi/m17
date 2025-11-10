Docs/
Flat Files/
Options/
Quotes
Quotes
S3
us_options_opra/quotes_v1
Top of book quotes with nanosecond timestamps from all U.S. options markets including CBOE, NYSE, and NASDAQ, made available as a daily downloadable S3 file.
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
Not included
Options Developer
Not included
Options Advanced
Your plan
End-of-day
Options Business
End-of-day
Plan Recency
Included in your Options plan (updated at 11a ET to include the previous day)

Plan
History
Options Basic
Not included
Options Starter
Not included
Options Developer
Not included
Options Advanced
Your plan
All history
Options Business
All history
Plan History
All history in your Options plan (records date back to March 7, 2022)

File Browser
Documentation
Documentation
Sample file
ticker	ask_exchange	ask_price	ask_size	bid_exchange	bid_price	bid_size	sequence_number	sip_timestamp
O:SPY241220P00720000	309	326.77	1	309	321.77	1	81779	1680010200067217664
O:SPY241220P00720000	309	328.28	1	303	318.28	2	152697	1680010200661050624
O:SPY241220P00720000	301	328.28	201	301	318.28	201	219334	1680010201264395520
O:SPY241220P00720000	301	328.28	201	301	320.13	200	226821	1680010201304631040
O:SPY241220P00720000	301	328.28	200	301	320.13	200	235757	1680010201377559552
O:SPY241220P00720000	302	328.41	1	301	320.13	200	271019	1680010201635999488
O:SPY241220P00720000	300	325.88	1	300	322.56	1	280897	1680010201690479360
O:SPY241220P00720000	301	325.88	200	301	322.56	200	281400	1680010201693857280
O:SPY241220P00720000	301	326.72	1	325	322.5	1	519037	1680010203073100288
O:SPY241220P00720000	301	326.72	201	301	321.72	201	531179	1680010203143363840
Columns
ask_exchange
integer
The ask exchange ID.
ask_price
number
The ask price.
ask_size
number
The ask size. This represents the number of round lot orders at the given ask price. The normal round lot size is 100 shares. An ask size of 2 means there are 200 shares available to purchase at the given ask price.
bid_exchange
integer
The bid exchange ID.
bid_price
number
The bid price.
bid_size
number
The bid size. This represents the number of round lot orders at the given bid price. The normal round lot size is 100 shares. A bid size of 2 means there are 200 shares for purchase at the given bid price.
sequence_number
integer
The sequence number represents the sequence in which trade events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11). Values reset after each trading session/day.
sip_timestamp
integer
The nanosecond accuracy SIP Unix Timestamp. This is the timestamp of when the SIP received this trade from the exchange which produced it.
ticker
string
The exchange symbol that this item is traded under.
