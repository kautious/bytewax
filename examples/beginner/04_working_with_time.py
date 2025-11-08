"""Working with Time - Time-Based Data Processing

This example demonstrates working with time-stamped data:
- Processing events with timestamps
- Sorting by time
- Grouping by time periods
- Basic time-based aggregations

Concepts introduced:
- Timestamps and datetime objects
- Time-based sorting
- Grouping events by time period
- Key extraction from timestamps
"""

from datetime import datetime, timedelta
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource, TestingSink, run_main


def example_1_timestamped_events():
    """Example 1: Processing timestamped events."""
    print("\n" + "=" * 60)
    print("Example 1: Timestamped Events")
    print("=" * 60)

    flow = Dataflow("timestamped_events")

    # Events with timestamps
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    events = [
        (base_time + timedelta(seconds=0), "user_login", "alice"),
        (base_time + timedelta(seconds=5), "page_view", "alice"),
        (base_time + timedelta(seconds=10), "user_login", "bob"),
        (base_time + timedelta(seconds=15), "page_view", "bob"),
        (base_time + timedelta(seconds=20), "purchase", "alice"),
        (base_time + timedelta(seconds=25), "page_view", "charlie"),
    ]

    s = op.input("events", flow, TestingSource(events))

    # Add formatted timestamp
    s = op.map(
        "format",
        s,
        lambda e: {
            "timestamp": e[0].strftime("%H:%M:%S"),
            "action": e[1],
            "user": e[2],
        },
    )

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("\nEvents:")
    for event in output:
        print(f"  {event['timestamp']} - {event['user']:8s} {event['action']}")


def example_2_sort_by_time():
    """Example 2: Sorting events by timestamp."""
    print("\n" + "=" * 60)
    print("Example 2: Sort Events by Time")
    print("=" * 60)

    flow = Dataflow("sort_by_time")

    # Events in random time order
    events = [
        (datetime(2024, 1, 1, 12, 0, 30), "Event C"),
        (datetime(2024, 1, 1, 12, 0, 10), "Event A"),
        (datetime(2024, 1, 1, 12, 0, 20), "Event B"),
        (datetime(2024, 1, 1, 12, 0, 50), "Event E"),
        (datetime(2024, 1, 1, 12, 0, 40), "Event D"),
    ]

    s = op.input("events", flow, TestingSource(events))

    # Collect all events
    s = op.key_on("global_key", s, lambda x: "all")
    s = op.fold_final("collect", s, list, lambda acc, x: acc + [x])

    # Sort by timestamp
    s = op.map_value("sort", s, lambda events: sorted(events, key=lambda e: e[0]))

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("\nOriginal order:")
    for ts, event in events:
        print(f"  {ts.strftime('%H:%M:%S')} - {event}")

    print("\nSorted order:")
    for ts, event in output[0][1]:
        print(f"  {ts.strftime('%H:%M:%S')} - {event}")


def example_3_group_by_hour():
    """Example 3: Group events by hour."""
    print("\n" + "=" * 60)
    print("Example 3: Group Events by Hour")
    print("=" * 60)

    flow = Dataflow("group_by_hour")

    # Events across different hours
    events = [
        (datetime(2024, 1, 1, 10, 15, 0), "morning_event_1"),
        (datetime(2024, 1, 1, 10, 45, 0), "morning_event_2"),
        (datetime(2024, 1, 1, 11, 10, 0), "late_morning_1"),
        (datetime(2024, 1, 1, 14, 20, 0), "afternoon_1"),
        (datetime(2024, 1, 1, 14, 50, 0), "afternoon_2"),
        (datetime(2024, 1, 1, 14, 55, 0), "afternoon_3"),
    ]

    s = op.input("events", flow, TestingSource(events))

    # Extract hour as key
    s = op.map("add_hour_key", s, lambda e: (e[0].hour, e[1]))

    # Count events per hour
    s = op.map("add_count", s, lambda pair: (pair[0], 1))
    s = op.reduce_final("count_per_hour", s, lambda acc, x: acc + x)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("\nEvents by hour:")
    for hour, count in sorted(output):
        print(f"  {hour:02d}:00 - {count} events")


def example_4_time_ranges():
    """Example 4: Filter events by time range."""
    print("\n" + "=" * 60)
    print("Example 4: Filter by Time Range")
    print("=" * 60)

    flow = Dataflow("time_range_filter")

    base = datetime(2024, 1, 1, 12, 0, 0)
    events = [
        (base + timedelta(minutes=i), f"event_{i}") for i in range(0, 60, 5)
    ]

    s = op.input("events", flow, TestingSource(events))

    # Define time range
    start_time = base + timedelta(minutes=15)
    end_time = base + timedelta(minutes=35)

    # Filter events in range
    s = op.filter(
        "in_range", s, lambda e: start_time <= e[0] <= end_time
    )

    # Format for output
    s = op.map(
        "format",
        s,
        lambda e: f"{e[0].strftime('%H:%M')} - {e[1]}",
    )

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print(f"\nTime range: {start_time.strftime('%H:%M')} to {end_time.strftime('%H:%M')}")
    print("\nEvents in range:")
    for event in output:
        print(f"  {event}")


def example_5_event_intervals():
    """Example 5: Calculate time intervals between events."""
    print("\n" + "=" * 60)
    print("Example 5: Time Intervals Between Events")
    print("=" * 60)

    flow = Dataflow("event_intervals")

    base = datetime(2024, 1, 1, 12, 0, 0)
    events = [
        (base, "Start"),
        (base + timedelta(seconds=10), "Event 1"),
        (base + timedelta(seconds=25), "Event 2"),
        (base + timedelta(seconds=30), "Event 3"),
        (base + timedelta(seconds=50), "Event 4"),
    ]

    s = op.input("events", flow, TestingSource(events))

    # Collect all events
    s = op.key_on("global", s, lambda x: "all")
    s = op.fold_final("collect", s, list, lambda acc, x: acc + [x])

    # Calculate intervals
    def calculate_intervals(events):
        intervals = []
        for i in range(1, len(events)):
            prev_time, prev_event = events[i - 1]
            curr_time, curr_event = events[i]
            interval = (curr_time - prev_time).total_seconds()
            intervals.append(
                (prev_event, curr_event, interval)
            )
        return intervals

    s = op.map_value("intervals", s, calculate_intervals)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("\nEvent intervals:")
    for prev, curr, interval in output[0][1]:
        print(f"  {prev} → {curr}: {interval:.0f} seconds")


def example_6_business_hours():
    """Example 6: Filter events during business hours."""
    print("\n" + "=" * 60)
    print("Example 6: Business Hours Filter")
    print("=" * 60)

    flow = Dataflow("business_hours")

    # Events throughout the day
    base = datetime(2024, 1, 1, 0, 0, 0)
    events = [
        (base.replace(hour=h), f"event_at_{h:02d}00")
        for h in [7, 9, 12, 15, 18, 20, 22]
    ]

    s = op.input("events", flow, TestingSource(events))

    # Filter: business hours (9 AM - 6 PM)
    s = op.filter("business_hours", s, lambda e: 9 <= e[0].hour < 18)

    # Format output
    s = op.map(
        "format",
        s,
        lambda e: f"{e[0].strftime('%I %p')} - {e[1]}",
    )

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("\nBusiness hours (9 AM - 6 PM):")
    for event in output:
        print(f"  {event}")

    print(f"\nTotal events during business hours: {len(output)}")


if __name__ == "__main__":
    print("\n⏰ Working with Time Examples")
    print("Learn time-based data processing")

    example_1_timestamped_events()
    example_2_sort_by_time()
    example_3_group_by_hour()
    example_4_time_ranges()
    example_5_event_intervals()
    example_6_business_hours()

    print("\n" + "=" * 60)
    print("✅ All time-based examples completed!")
    print("=" * 60)


"""
Key Takeaways:

1. TIMESTAMPS:
   - Use Python datetime objects
   - Common format: datetime(year, month, day, hour, minute, second)
   - timedelta for time arithmetic

2. TIME-BASED KEYS:
   - Extract time components (hour, day, etc.) as keys
   - Group events by time period
   - Use for time-based aggregations

3. FILTERING BY TIME:
   - Compare timestamps with <= and >=
   - Filter by time range
   - Filter by time properties (hour, day of week, etc.)

4. COMMON PATTERNS:
   - Group by time period → Count
   - Sort by timestamp
   - Filter by time range
   - Calculate intervals

5. NEXT STEPS:
   - For more advanced time processing, see windowing examples
   - Window operators provide better time-based grouping
   - Consider timezones for production systems

Practice exercises:
1. Group events by day of week
2. Find the busiest hour
3. Calculate average time between events
4. Detect events outside normal hours
"""
