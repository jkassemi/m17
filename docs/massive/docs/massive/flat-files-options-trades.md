Docs/
Flat Files/
Options/
Trades
Trades
S3
us_options_opra/trades_v1
Tick-level trades with nanosecond timestamps from all U.S. options markets including CBOE, NYSE, and NASDAQ, made available as a daily downloadable S3 file.
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
End-of-day
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
4 years
Options Advanced
Your plan
All history
Options Business
All history
Plan History
All history in your Options plan (records date back to June 2, 2014)

File Browser
Documentation
Documentation
Sample file
ticker	conditions	correction	exchange	participant_timestamp	price	sip_timestamp	size
O:SPY230327P00390000	232		312	1678715620948000000	11.82	1678715620948000000	1
O:SPY230327P00390000	209		325	1678716040304000000	11.07	1678716040304000000	15
O:SPY230327P00390000	227		302	1678716830560000000	11.13	1678716830560000000	1
O:SPY230327P00390000	209		325	1678717075708000000	11.67	1678717075708000000	1
O:SPY230327P00390000	227		300	1678719793268000000	8.9	1678719793268000000	1
O:SPY230327P00390000	227		302	1678719975903000000	9.35	1678719975903000000	2
O:SPY230327P00390000	227		302	1678719975903000000	9.35	1678719975903000000	2
O:SPY230327P00390000	227		302	1678719975903000000	9.35	1678719975903000000	1
O:SPY230327P00390000	209		313	1678720757648000000	8.37	1678720757648000000	5
O:SPY230327P00390000	227		300	1678721008544000000	8.96	1678721008544000000	1
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
price
number
The price of the trade. This is the actual dollar value per whole share of this trade. A trade of 100 shares with a price of $2.00 would be worth a total dollar value of $200.00.
sip_timestamp
integer
The nanosecond accuracy SIP Unix Timestamp. This is the timestamp of when the SIP received this trade from the exchange which produced it.
size
number
The size of a trade (also known as volume).
ticker
string
The exchange symbol that this item is traded under.
