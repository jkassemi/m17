<command>cat README.md</command>
<command>cat SPEC.md</command>

<command>python context.py</command>

1) I'd like to get started with ingestion of flat file data from massive to fill the configured time windows (and to maintain the loop that updates as new data becomes available). We need the historical data (we can't rely on seeing a trade with the websocket). While I'd like to get started with equities, we need to consider the pipeline here and make sure the code we use can utilized by the WS service as well.

2) Please consider the specification and assess against the in-progress implementation.

3. Break remaining tasks into a series of small steps.

4. Running the ingestion of flatfiles now. It seems like this should take much less time than it is, but they are large files. Is it possible to stream the compressed content and process the trades as we're doing so, displaying progress along the way?
