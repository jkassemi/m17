Docs/
WebSocket/
Stocks/
Stocks Overview
Stocks Overview
At Massive.com, we provide real-time streaming access to U.S. stock market data through WebSockets, delivering a continuous flow of updates directly to your applications. Our WebSocket feeds include trades, quotes, aggregates (bars), limit-up/limit-down (LULD) events, and Fair Market Value (FMV) measurements, enabling developers, traders, and analysts to receive live, push-based market data with minimal latency. By tapping into these streaming endpoints, you can power dynamic dashboards, feed algorithmic trading strategies, and monitor market conditions as they unfold -- without the need for repeated requests.

Market Hours and Timezone
All stock market data delivered over WebSockets follows the standard U.S. equity trading sessions in Eastern Time (ET):

Pre-Market: 4:00 AM to 9:30 AM ET
Regular Market: 9:30 AM to 4:00 PM ET
After-Hours: 4:00 PM to 8:00 PM ET
While streaming endpoints remain active outside regular hours, the frequency and type of updates may vary depending on market activity and the data feed in question. All timestamps are provided as Unix timestamps (seconds since epoch, UTC). When converting these timestamps into human-readable form (e.g., market open at 9:30 AM), remember they represent UTC time, not Eastern Time (ET). To correctly align data with market hours or dates, you'll need to explicitly convert timestamps from UTC to ET during your analysis.

Infrastructure and Reliability
Our WebSocket infrastructure is engineered for speed, stability, and scalability. By co-locating servers with exchanges and Securities Information Processors (SIPs), we minimize latency and ensure that you receive updates as quickly as possible. Redundant data centers and load balancing further enhance reliability, allowing us to maintain steady data delivery even under high load or challenging network conditions.

This real-time architecture ensures seamless, continuous data streams, ideal for latency-sensitive applications such as algorithmic trading, live charting, and event-driven analyses.

Data Flow: From Exchanges to You
Massive.com’s WebSocket feeds draw data from the same robust sources as our REST endpoints. We combine direct connections to all major U.S. stock exchanges with SIP-consolidated feeds, ensuring that you receive the most accurate, timely, and comprehensive market data available.

As soon as trades, quotes, or other market events are published by the exchanges and consolidated by the SIPs, they are relayed through our infrastructure and pushed over our WebSocket channels to your subscribed clients. This near-instantaneous delivery supports real-time decision-making and dynamic updates to your trading or analytical systems.

Available WebSocket Feeds for Stocks
Our WebSocket channels deliver a broad range of streaming data, covering all critical aspects of market activity:

Aggregates (Per Minute): Receive OHLC (Open, High, Low, Close) bars updated every minute. Ideal for intraday charting, real-time technical analysis, and monitoring market trends.
Aggregates (Per Second): Obtain second-by-second OHLC bars for ultra-fine-grained analysis. Useful for high-frequency strategies, liquidity assessments, and rapid response trading models.
Trades: Stream every executed trade in real-time, including price, size, exchange, and conditions. Perfect for tick-level analyses, order flow studies, and highly responsive trading algorithms.
Quotes: Access NBBO (National Best Bid and Offer) quotes as they update. This feed supports monitoring the evolving price landscape, evaluating spreads, and identifying liquidity conditions.
Limit Up - Limit Down (LULD): Track real-time volatility safeguards and price bands triggered by regulatory mechanisms. Useful for detecting trading halts, resumption signals, and understanding rapid market movements.
Fair Market Value (FMV): Obtain our proprietary real-time FMV metric, exclusively available to Business plan users, offering an algorithmically derived estimate of a security’s fair market price.
By subscribing to these WebSocket feeds, you gain uninterrupted, event-driven data flow, eliminating the overhead of polling for updates and ensuring that your applications always stay in sync with the latest market conditions.

Next Steps
Leverage our documentation to integrate these WebSocket feeds into your applications. With a simple subscription model, you can dynamically select which tickers and data streams you need, enabling tasks such as:

Powering live dashboards and visualizations
Feeding algorithmic models with instantaneous updates
Conducting real-time risk management and compliance checks
Enhancing trading platforms with low-latency insights
By utilizing Massive.com’s stock WebSocket feeds, you position your applications at the cutting edge of real-time market intelligence, empowering rapid, data-driven decision-making and innovation in the U.S. equity markets.
