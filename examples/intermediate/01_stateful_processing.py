"""Stateful Processing - Maintaining State Across Items

This example demonstrates stateful operators that maintain state across items:
- stateful_map: Transform with state (1-to-1)
- stateful_flat_map: Transform with state (1-to-many)
- fold: Accumulate values over time
- State management patterns

Concepts introduced:
- Stateful transformations
- State initialization
- State updates
- Per-key state management
"""

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource, TestingSink, run_main
from typing import Optional, Tuple


def example_1_running_total():
    """Example 1: Calculate running total with stateful_map."""
    print("\n" + "=" * 60)
    print("Example 1: Running Total with stateful_map")
    print("=" * 60)

    flow = Dataflow("running_total")

    # Input: numbers
    numbers = [10, 20, 30, 40, 50]
    s = op.input("numbers", flow, TestingSource(numbers))

    # Add a key (all items share same key for global total)
    s = op.key_on("add_key", s, lambda x: "total")

    # Stateful mapper: maintain running sum
    def running_sum(state: Optional[int], value: int) -> Tuple[int, int]:
        """
        Args:
            state: Previous sum (None if first time)
            value: Current number

        Returns:
            (new_state, output): New sum and current total
        """
        if state is None:
            state = 0
        new_state = state + value
        return (new_state, new_state)  # Return (state, output)

    s = op.stateful_map("running_sum", s, running_sum)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print(f"\nInput:  {numbers}")
    print("Running totals:")
    for key, total in output:
        print(f"  {total}")


def example_2_moving_average():
    """Example 2: Calculate moving average."""
    print("\n" + "=" * 60)
    print("Example 2: Moving Average (Window Size 3)")
    print("=" * 60)

    flow = Dataflow("moving_average")

    values = [10, 20, 30, 40, 50, 60, 70, 80]
    s = op.input("values", flow, TestingSource(values))

    s = op.key_on("key", s, lambda x: "avg")

    # State: list of recent values (max 3)
    def moving_avg(state: Optional[list], value: int) -> Tuple[list, Optional[float]]:
        if state is None:
            state = []

        # Add new value
        state.append(value)

        # Keep only last 3 values
        state = state[-3:]

        # Calculate average
        avg = sum(state) / len(state) if state else None

        return (state, avg)

    s = op.stateful_map("avg", s, moving_avg)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print(f"\nInput values: {values}")
    print("Moving averages (window=3):")
    for _, avg in output:
        if avg:
            print(f"  {avg:.1f}")


def example_3_per_user_state():
    """Example 3: Maintain state per user."""
    print("\n" + "=" * 60)
    print("Example 3: Per-User Session Tracking")
    print("=" * 60)

    flow = Dataflow("per_user_state")

    # User events
    events = [
        ("alice", "login"),
        ("bob", "login"),
        ("alice", "click"),
        ("alice", "click"),
        ("bob", "click"),
        ("alice", "purchase"),
        ("bob", "click"),
        ("bob", "purchase"),
    ]

    s = op.input("events", flow, TestingSource(events))

    # Events are already keyed by user (first element)

    # Track action count per user
    def track_actions(
        state: Optional[dict], action: str
    ) -> Tuple[dict, dict]:
        if state is None:
            state = {}

        # Increment action count
        state[action] = state.get(action, 0) + 1

        return (state, state.copy())  # Return copy for output

    s = op.stateful_map("track", s, track_actions)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("\nUser action tracking:")
    # Get final state for each user
    user_states = {}
    for user, actions in output:
        user_states[user] = actions

    for user, actions in sorted(user_states.items()):
        print(f"\n{user}:")
        for action, count in sorted(actions.items()):
            print(f"  {action}: {count}")


def example_4_sequence_detection():
    """Example 4: Detect sequences of events."""
    print("\n" + "=" * 60)
    print("Example 4: Detect Event Sequences")
    print("=" * 60)

    flow = Dataflow("sequence_detection")

    # Events per user
    events = [
        ("alice", "view_product"),
        ("bob", "view_product"),
        ("alice", "add_to_cart"),
        ("alice", "checkout"),  # Complete sequence!
        ("bob", "view_product"),
        ("charlie", "view_product"),
        ("charlie", "add_to_cart"),
        ("charlie", "checkout"),  # Complete sequence!
    ]

    s = op.input("events", flow, TestingSource(events))

    # Detect complete purchase sequence
    def detect_sequence(
        state: Optional[list], event: str
    ) -> Tuple[list, Optional[str]]:
        if state is None:
            state = []

        state.append(event)

        # Check for complete sequence
        target = ["view_product", "add_to_cart", "checkout"]
        if state[-3:] == target:
            return (state, "COMPLETE_PURCHASE")  # Sequence detected!
        else:
            return (state, None)

    s = op.stateful_map("detect", s, detect_sequence)

    # Filter to only completed sequences
    s = op.filter("completed_only", s, lambda x: x[1] is not None)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("\nComplete purchase sequences detected:")
    for user, status in output:
        print(f"  {user}: {status}")


def example_5_deduplication():
    """Example 5: Deduplicate with state."""
    print("\n" + "=" * 60)
    print("Example 5: Deduplicate Items")
    print("=" * 60)

    flow = Dataflow("dedup")

    # Events with duplicates
    events = [
        ("user1", "event_A"),
        ("user1", "event_B"),
        ("user1", "event_A"),  # Duplicate
        ("user2", "event_A"),
        ("user1", "event_C"),
        ("user2", "event_A"),  # Duplicate
        ("user1", "event_B"),  # Duplicate
    ]

    s = op.input("events", flow, TestingSource(events))

    # Track seen events per user
    def dedup(
        state: Optional[set], event: str
    ) -> Tuple[set, Optional[str]]:
        if state is None:
            state = set()

        if event in state:
            # Already seen, filter it out
            return (state, None)
        else:
            # New event, keep it
            state.add(event)
            return (state, event)

    s = op.stateful_map("dedup", s, dedup)

    # Filter out None values
    s = op.filter("remove_none", s, lambda x: x[1] is not None)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("\nDeduplicated events:")
    for user, event in output:
        print(f"  {user}: {event}")


def example_6_session_timeout():
    """Example 6: Track sessions with timeout."""
    print("\n" + "=" * 60)
    print("Example 6: Session Tracking with Activity")
    print("=" * 60)

    flow = Dataflow("session_tracking")

    # User activity events
    events = [
        ("alice", "click"),
        ("alice", "click"),
        ("bob", "click"),
        ("alice", "click"),
        ("bob", "click"),
        ("bob", "click"),
        ("charlie", "click"),
    ]

    s = op.input("events", flow, TestingSource(events))

    # Count events per user
    def count_events(
        state: Optional[int], event: str
    ) -> Tuple[int, int]:
        if state is None:
            state = 0
        state += 1
        return (state, state)

    s = op.stateful_map("count", s, count_events)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("\nEvent counts per user:")
    # Get final counts
    user_counts = {}
    for user, count in output:
        user_counts[user] = count

    for user, count in sorted(user_counts.items()):
        print(f"  {user}: {count} events")


if __name__ == "__main__":
    print("\n📊 Stateful Processing Examples")
    print("Learn to maintain state across items")

    example_1_running_total()
    example_2_moving_average()
    example_3_per_user_state()
    example_4_sequence_detection()
    example_5_deduplication()
    example_6_session_timeout()

    print("\n" + "=" * 60)
    print("✅ All stateful processing examples completed!")
    print("=" * 60)


"""
Key Takeaways:

1. STATEFUL_MAP:
   - Signature: (state, value) -> (new_state, output)
   - State persists across items with same key
   - State=None for first item
   - Returns tuple: (updated_state, output_value)

2. STATE PER KEY:
   - Each key has its own independent state
   - Use key_on to assign keys
   - State isolated between keys

3. STATE TYPES:
   - Simple: int, float, string
   - Collections: list, set, dict
   - Custom: any Python object

4. COMMON PATTERNS:
   - Running totals/aggregations
   - Moving windows (recent N items)
   - Sequence detection
   - Deduplication
   - Session tracking

5. STATE INITIALIZATION:
   - Check if state is None
   - Initialize with appropriate default
   - Handle edge cases

6. BEST PRACTICES:
   - Keep state manageable in size
   - Consider memory implications
   - Use appropriate data structures
   - Return copies if needed

Next steps:
- See windowing examples for time-based state
- See fold operators for accumulation
- Consider recovery for stateful operators
"""
