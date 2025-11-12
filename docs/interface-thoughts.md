Help me work through details of the following interface. Pros and cons. I think we're at a place where we could build and start using it for tomorrow's trading day:

1. user inputs symbol.
    fetch the minimum date of the underlying quote data set as left
    set the right to the current day
    include a summary of what details we have for the range (underlying quotes, underlying trades, options quotes, options trades, and enrichment status)
    plot underlying price data (dynamic resolution)

2. user can select bounds to zoom starting with left (moving selection bar through the plot with <h> and <l>. details of selected range are included on the screen.

3. unless an aggregation window is selected, the user is just navigating the price (dynamic resolution, but aggregated only for display resolution, not intentionally).
<o> key zooms back out to minimum to current day window. <i> goes back to the last zoomed in region (pipelines should continue processing when zoomed out.

4. user can, at any time, select an aggregation window size with the <x> key, which will provide a menu for selection for valid window sizes (5m, 10m, 15m, 30m, None). upon selection of an aggregation window, we pipeline trades through the aggressor categorization, greeks, and aggregations. the user can move through the aggregation details by using the <j> and <k> keys.

5. the user can cancel the current in-progress data pipelines at any time by selecting a None aggregation window size.

6. the user can select a new symbol with the <s> key, which should prompt them for a new symbol to set as active.

6. if selection bar is moved past the bounds of the available data (this should be allowed, but once), they're enabling realtime. The status bar should indicate this would be the selection before hitting enter.

in realtime mode the aggregations are automatically updated, and the user can page through them with <j> and <k>. If <j> is used _past_ the last avilable frame in realtime mode, the latest aggregation window is displayed and will be refreshed as soon as the next aggregation window is fully available.

7. <c> key automatically copies the contents of the currently selected aggregation window (which includes all calculated aggregation properties in a human readable format).

Notes:

- this is the first iteration of the interface. we may (and likely will) extend the plot to include the information from the aggregation windows. it might be worth stubbing out including DADVV so we can make sure we support multiple independent axes.  
- In general, it may not be necessary to actually _process_ the data at all (aggressor, greeks, etc) until we've requested it from the TUI. That'll help us limit our data processing pipeline considerably. We should make sure the orchestrator is capturing the underlying trade and quote data, but the processing pipeline can be initiated through this interface. In other words, we don't need to classify the aggressor, greeks, etc until the selection is activated by the user, as long as we have the source data. This will likely change in the future, but we'll have more resources for hardware at that point.
- underlying symbol configuration from the config file should be removed. we should maintain a TUI state file that includes the previous settings so we can resume on rerun.
