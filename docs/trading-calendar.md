Crate trading_calendar Copy item path
Source
Search
Settings
Help

Summary
Trading Calendar
A comprehensive trading calendar for global financial markets, providing holidays, trading hours, and early close information.

Features
🌍 Multiple Markets: NYSE, NASDAQ, LSE, TSE, TSX
⏰ Trading Hours: Regular, pre-market, and after-hours sessions
📅 Holiday Detection: All market holidays with weekend adjustments
🕐 Early Closes: Half-day schedules (Christmas Eve, Black Friday, etc.)
🌐 Timezone Support: Automatic handling of market timezones
🚀 Performance: Efficient LRU caching
🔒 Thread Safe: Concurrent access support
📆 2020-2030 Support: Comprehensive holiday calendars
Quick Start
use trading_calendar::{TradingCalendar, Market};

fn main() -> trading_calendar::Result<()> {
    let nyse = TradingCalendar::new(Market::NYSE)?;
     
    // Check if market is open
    if nyse.is_open_now()? {
        println!("NYSE is open for trading!");
    }
     
    // Get next market open
    let next_open = nyse.next_open()?;
    println!("NYSE opens: {}", next_open);
     
    // Check specific date
    let christmas = chrono::NaiveDate::from_ymd_opt(2025, 12, 25).unwrap();
    if !nyse.is_trading_day(christmas)? {
        println!("Market closed on Christmas");
    }
     
    Ok(())
}
Working with Different Markets
use trading_calendar::{TradingCalendar, Market};

// Create calendars for different markets
let nyse = TradingCalendar::new(Market::NYSE)?;
let lse = TradingCalendar::new(Market::LSE)?;
let tse = TradingCalendar::new(Market::TSE)?;

// Each market has its own timezone
assert_eq!(nyse.timezone().name(), "America/New_York");
assert_eq!(lse.timezone().name(), "Europe/London");
assert_eq!(tse.timezone().name(), "Asia/Tokyo");
Handling Early Close Days
use trading_calendar::{TradingCalendar, Market};
use chrono::NaiveDate;

let nyse = TradingCalendar::new(Market::NYSE)?;
let christmas_eve = NaiveDate::from_ymd_opt(2025, 12, 24).unwrap();

let hours = nyse.trading_hours(christmas_eve);
if hours.is_early_close() {
    println!("Market closes early at {}", hours.market_close());
}
Supported Markets
Market	Regular Hours (Local)	Pre-Market	After-Hours	Status
NYSE	9:30 AM - 4:00 PM ET	4:00 AM - 9:30 AM	4:00 PM - 8:00 PM	✅ Full Support
NASDAQ	9:30 AM - 4:00 PM ET	4:00 AM - 9:30 AM	4:00 PM - 8:00 PM	✅ Full Support
LSE	8:00 AM - 4:30 PM GMT	-	-	✅ Full Support
TSE	9:00 AM - 3:00 PM JST	-	-	✅ Full Support
TSX	9:30 AM - 4:00 PM ET	-	-	✅ Full Support
Thread Safety
The TradingCalendar is thread-safe and can be shared across threads:

use std::sync::Arc;
use trading_calendar::{TradingCalendar, Market};

fn main() -> trading_calendar::Result<()> {
    let calendar = Arc::new(TradingCalendar::new(Market::NYSE)?);

    // Share calendar across threads safely
    let cal_clone = Arc::clone(&calendar);
    std::thread::spawn(move || {
        let is_open = cal_clone.is_open_now().unwrap_or(false);
    });
     
    Ok(())
}
Error Handling
The library uses proper error handling with Result types:

use trading_calendar::{TradingCalendar, Market, CalendarError};

fn main() -> trading_calendar::Result<()> {
    let calendar = TradingCalendar::new(Market::NYSE)?;

    // Check for unsupported years
    match calendar.is_trading_day(chrono::NaiveDate::from_ymd_opt(2019, 1, 1).unwrap()) {
        Ok(is_trading) => println!("Is trading day: {}", is_trading),
        Err(CalendarError::DateOutOfRange(date)) => println!("Date {} not supported", date),
        Err(e) => eprintln!("Error: {}", e),
    }
     
    Ok(())
}
Performance
The library uses efficient caching to ensure optimal performance:

Holiday calculations are cached per year using LRU cache
Thread-safe concurrent access with proper eviction
Minimal allocations with optimized data structures
License
Licensed under either of:

Apache License, Version 2.0 (LICENSE-APACHE)
MIT license (LICENSE-MIT)
at your option.

Re-exports
pub use calendar::TradingCalendar;
pub use error::CalendarError;
pub use error::Result;
pub use markets::Market;
pub use schedule::Session;
pub use schedule::TradingHours;
Modules
calendar
Main trading calendar implementation
constants
Market-specific constants
error
Error types for the trading calendar
markets
Market definitions and implementations
schedule
Trading hours and session definitions
utils
Utility functions for calendar operations
Structs
DateTime
ISO 8601 combined date and time with time zone.
Holiday
Holiday information
NaiveDate
ISO 8601 calendar date without timezone. Allows for every proleptic Gregorian date from Jan 1, 262145 BCE to Dec 31, 262143 CE. Also supports the conversion from ISO 8601 ordinal and week date.
NaiveTime
ISO 8601 time without timezone. Allows for the nanosecond precision and optional leap second representation.
Utc
The UTC time zone. This is the most efficient time zone when you don’t need the local time. It is also used as an offset (which is also a dummy type).
Constants
MAX_YEAR
Maximum supported year
MIN_YEAR
Minimum supported year
