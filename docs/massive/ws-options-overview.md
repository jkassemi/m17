Docs/
WebSocket/
Options/
Options Overview
Options Overview
At Massive.com, we provide real-time streaming access to U.S. options market data through WebSockets, allowing you to receive a continuous flow of updates directly into your applications. Our WebSocket feeds cover trades, quotes, and aggregate bars (both per-minute and per-second), as well as Fair Market Value (FMV) for eligible subscribers. With these streaming endpoints, developers, traders, and analysts can monitor live market conditions, power complex strategies, and update visualizations dynamically -- without the need for repeated requests.

Market Hours and Timezone
U.S. options trading primarily occurs during standard equity market sessions:

Regular Market Hours: Monday through Friday, 9:30 AM to 4:00 PM Eastern Time (ET)
While some limited trading may occur outside these hours, options activity is concentrated within the regular session. All timestamps are provided as Unix timestamps (seconds since epoch, UTC). When converting these timestamps into human-readable form (e.g., market open at 9:30 AM), remember they represent UTC time, not Eastern Time (ET). To correctly align data with market hours or dates, you'll need to explicitly convert timestamps from UTC to ET during your analysis.

Infrastructure and Reliability
Our WebSocket infrastructure is specifically engineered for the high-throughput and data-intensive nature of the U.S. options market. By co-locating with the Options Price Reporting Authority (OPRA), we minimize latency and ensure you receive updates as they are consolidated from all U.S. options exchanges. Redundant systems and load balancing further enhance reliability, allowing you to maintain steady, low-latency data streams even under peak market conditions.

This real-time delivery architecture supports latency-sensitive applications, such as algorithmic trading and intraday analytics, providing you with a continuous feed of fresh market data as it happens.

Data Flow: From Exchanges to You
All U.S. options exchanges report their trades and quotes to OPRA, which consolidates them into a single, authoritative feed. Massive.com connects directly to OPRA, ensuring you receive the official National Best Bid and Offer (NBBO) and complete trade information for all listed options contracts. As soon as updates are available, we push them over your subscribed WebSocket channels, delivering near-instantaneous market insights to your applications.

Available WebSocket Feeds for Options
Our WebSocket channels deliver a range of streaming options data, providing a complete picture of real-time market activity:

Aggregates (Per Minute): Receive minute-by-minute OHLC bars constructed from qualifying trades. Ideal for real-time charting, trend detection, and short-term strategy development.
Aggregates (Per Second): Obtain second-level OHLC bars for fine-grained, high-frequency insights. Perfect for algorithmic traders and advanced analytics requiring precise, low-latency data.
Trades: Stream every executed option trade in real-time, including price, size, and exchange details. This granular feed supports tick-level analyses, order flow modeling, and ultra-responsive trading algorithms.
Quotes: Access NBBO quotes as they update across all U.S. options exchanges. Monitor evolving price landscapes, identify liquidity conditions, and make rapid, informed decisions. Note: To maintain data quality and manage bandwidth, a limit of 1,000 simultaneous option contract subscriptions per connection applies.
Fair Market Value (FMV): Obtain our proprietary real-time FMV metric, exclusively available to Business plan users, providing an algorithmically derived estimate of an option’s fair market price and enhancing the precision of your analysis.
By subscribing to these WebSocket feeds, you gain direct, event-driven access to market data without continuously polling for updates. This streamlined approach allows your applications to remain synchronized with live market conditions, supporting agile trading and insightful research.

Next Steps
Utilize our comprehensive documentation to integrate these WebSocket feeds into your applications. With a simple subscription model, you can focus on the data streams and contracts that matter most to you, enabling tasks such as:

Powering live option dashboards and analytics
Supporting algorithmic models that react instantly to market shifts
Enhancing risk management and position monitoring with real-time updates
Driving sophisticated options trading strategies with low-latency data
By leveraging Massive.com’s options WebSocket feeds, you equip your applications to respond dynamically to evolving market conditions, unlocking real-time insights that inform competitive and profitable trading decisions.
