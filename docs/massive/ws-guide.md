WebSocket Quickstart
Get started with the Massive.com WebSocket API
A screenshot displaying a JSON response with options data on one side and a graphical user interface presenting the same information, illustrating the types of applications you can build with Massive.com data

The Massive.com WebSocket API provides streaming market data from major U.S. exchanges and other sources. For most users, integrating our official client libraries is the easiest and quickest way to handle authentication, subscription management, and message parsing. To get started, you will need to sign up for an account and authenticate your requests using an API key.

If you're building a real-time dashboard or trading system, WebSockets are ideal for event-driven updates. For one-time queries or smaller, filtered data requests, consider using our REST API. And if you're looking to download historical data in CSV format, our Flat Files are available via a web-based browser or S3-compatible tools.

In this guide, we will walk through the mechanics of connecting and exchanging messages over a WebSocket connection so you can fully understand what's happening behind the scenes. We will use a command-line WebSocket client (wscat) to demonstrate these steps at a low level.

Requirements & Approach
In a production environment, you will likely never manually manage WebSocket connections and message flows as shown here. Instead, you would use one of our official client libraries, which encapsulate these details, handling tasks such as:

Establishing and maintaining the WebSocket connection.
Sending authentication messages automatically.
Subscribing and unsubscribing to feeds.
Efficiently parsing and handling incoming data.
By following along with the steps below using wscat, you'll gain insight into how our WebSocket streams operate under the hood, giving you confidence in what our client libraries are doing on your behalf.

Preparing Your Environment:

Ensure you have Node.js installed.
At your terminal, install wscat, a handy tool for interacting with WebSocket endpoints:

Install wscat


npm install -g wscat
Connecting to the WebSocket
Massive.com offers both delayed and real-time data feeds depending on your needs.

15-minute Delayed Data:


Connect to 15-minute delayed feed


wscat -c wss://delayed.massive.com/stocks
Real-Time Data:


Connect to real-time feed


wscat -c wss://socket.massive.com/stocks
Server to Client (Upon Successful Connection):


Receive Successful Connection Message


[{
  "ev":"status",
  "status":"connected",
  "message":"Connected Successfully"
}]
Note on Connections: By default, one concurrent WebSocket connection per asset class is allowed. If you require multiple simultaneous connections for the same asset class, please contact support.

Authentication
After connecting, you must authenticate with your API key before subscribing to data feeds.

Client to Server (Send Your API Key):


Send Authentication Request


{
  "action":"auth",
  "params":"qRzMAhv3j6qfOvMNJ7LhO4qklEqRc0Rx"
}
Server to Client (Successful Authentication):


Receive Successful Authentication Message


[{
  "ev":"status",
  "status":"auth_success",
  "message":"authenticated"
}]
Subscribing to Data Feeds
Once authenticated, you can subscribe to various channels. Below is an example subscribing to Stocks Aggregates (Per Minute) updates for AAPL and MSFT:

Client to Server (Subscription Request):


Send Subscription Request


{
  "action":"subscribe",
  "params":"AM.AAPL,AM.MSFT"
}
Server to Client (Data Messages): After subscribing, you will receive updates as they occur. For example, a real-time aggregate per minute (AM) message might look like this:


Receive Data Messages for Your Subscription


[
  {
    "ev": "AM",
    "sym": "AAPL",
    "v": 12345,
    "o": 150.85,
    "c": 152.90,
    "h": 153.17,
    "l": 150.50,
    "a": 151.87,
    "s": 1611082800000,
    "e": 1611082860000
  }
]
Field Explanations:

ev: Event type (e.g., "AM" for aggregate minute bars)
sym: Symbol (e.g., "AAPL")
v: Volume in the aggregate period
o, c, h, l: Open, Close, High, Low prices
a: VWAP (Volume Weighted Average Price)
s: Start timestamp of the aggregate period (Unix ms)
e: End timestamp of the aggregate period (Unix ms)
Message Formats & Multiple Events
During high-volume periods, the server may bundle multiple events into a single JSON array.

Example with Multiple Trade Events:


Server to Client (Data Messages)


[
  {
    "ev": "T",        
    "sym": "MSFT",    
    "i": "50578",     
    "x": 4,           
    "p": 215.9721,    
    "s": 100,         
    "t": 1611082428813,
    "z": 3            
  },
  {
    "ev": "T",
    "sym": "MSFT",
    "i": "12856",
    "x": 4,
    "p": 215.989,
    "s": 1,
    "c": [37],        
    "t": 1611082428814,
    "z": 3
  }
]
By examining the ev field and other attributes, you can distinguish event types and integrate the data into your application logic.

Performance & Latency Considerations
Handling streaming market data efficiently is key:

Process messages quickly to avoid server-side buffering.
If you consistently receive more data than you can handle, reduce subscriptions.
Use a wired connection and avoiding Wifi and VPNs can help reduce latency.
If the server detects slow message consumption, it may disconnect to maintain overall system performance.

Explore Client Libraries
In practice, you’ll likely rely on our client libraries for handling these details automatically. They simplify:

Authentication
Subscription management
Message parsing
Error handling and reconnect logic
Available libraries:

Python: GitHub - Python Client
Go: GitHub - Go Client
Kotlin: GitHub - JVM Client
JavaScript: GitHub - JavaScript Client
Next Steps
By following this guide and understanding the underlying message flow, you can appreciate what happens when you connect, authenticate, and subscribe to data feeds. While this manual approach is useful for learning, our client libraries are the recommended choice for actual development and production use. With this knowledge in hand, explore the rest of our documentation to discover more channels, event types, and advanced capabilities.
