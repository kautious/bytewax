"""Integration tests for complex dataflow scenarios."""

from dataclasses import dataclass
from datetime import timedelta

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_complex_pipeline_with_multiple_transformations():
    """Test a complex pipeline with many transformation steps."""
    out = []

    flow = Dataflow("complex_pipeline")
    # Start with numbers 1-10
    s = op.input("inp", flow, TestingSource(range(1, 11)))
    # Double them
    s = op.map("double", s, lambda x: x * 2)
    # Filter evens only (all should be even after doubling)
    s = op.filter("evens", s, lambda x: x % 2 == 0)
    # Add 10 to each
    s = op.map("add_10", s, lambda x: x + 10)
    # Convert to strings
    s = op.map("to_string", s, lambda x: f"num_{x}")
    op.output("out", s, TestingSink(out))

    run_main(flow)
    expected = [f"num_{x * 2 + 10}" for x in range(1, 11)]
    assert out == expected


def test_branching_and_merging():
    """Test splitting stream and merging back."""
    out = []

    flow = Dataflow("branch_merge")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5, 6]))
    # Split into evens and odds
    branches = op.branch("split", s, lambda x: x % 2 == 0)

    # Process evens - multiply by 10
    evens = op.map("proc_evens", branches.trues, lambda x: x * 10)

    # Process odds - multiply by 100
    odds = op.map("proc_odds", branches.falses, lambda x: x * 100)

    # Merge back together
    merged = op.merge("merge", evens, odds)
    op.output("out", merged, TestingSink(out))

    run_main(flow)
    # Should have processed values, order not guaranteed
    assert sorted(out) == [20, 40, 60, 100, 300, 500]


def test_keyed_aggregation_pipeline():
    """Test aggregation on keyed streams."""
    out = []

    @dataclass
    class Event:
        user_id: str
        amount: int

    events = [
        Event("alice", 10),
        Event("bob", 20),
        Event("alice", 15),
        Event("bob", 25),
        Event("alice", 5),
    ]

    flow = Dataflow("keyed_agg")
    s = op.input("inp", flow, TestingSource(events))
    # Key by user_id
    keyed = op.key_on("key", s, lambda e: e.user_id)
    # Extract amounts
    amounts = op.map_value("amounts", keyed, lambda e: e.amount)
    # Sum all amounts per user
    totals = op.reduce_final("sum", amounts, lambda acc, x: acc + x)
    op.output("out", totals, TestingSink(out))

    run_main(flow)
    # Sort for deterministic comparison
    result = sorted(out)
    assert result == [("alice", 30), ("bob", 45)]


def test_multiple_joins():
    """Test joining multiple streams together."""
    out = []

    flow = Dataflow("multi_join")

    # Stream 1: user names
    s1 = op.input(
        "names",
        flow,
        TestingSource([("user1", "Alice"), ("user2", "Bob"), ("user3", "Charlie")]),
    )

    # Stream 2: user ages
    s2 = op.input(
        "ages",
        flow,
        TestingSource([("user1", 25), ("user2", 30), ("user3", 35)]),
    )

    # Stream 3: user cities
    s3 = op.input(
        "cities",
        flow,
        TestingSource([("user1", "NYC"), ("user2", "LA"), ("user3", "SF")]),
    )

    # Join all three
    joined = op.join("join", s1, s2, s3)
    op.output("out", joined, TestingSink(out))

    run_main(flow)
    # Sort for deterministic comparison
    result = sorted(out)
    assert result == [
        ("user1", ("Alice", 25, "NYC")),
        ("user2", ("Bob", 30, "LA")),
        ("user3", ("Charlie", 35, "SF")),
    ]


def test_stateful_processing_with_counting():
    """Test stateful processing that counts items per key."""
    out = []

    def counter(state, value):
        if state is None:
            state = 0
        state += 1
        return (state, (value, state))

    flow = Dataflow("stateful_count")
    s = op.input(
        "inp",
        flow,
        TestingSource(
            [("a", 1), ("b", 2), ("a", 3), ("b", 4), ("a", 5), ("c", 6)]
        ),
    )
    counted = op.stateful_map("count", s, counter)
    op.output("out", counted, TestingSink(out))

    run_main(flow)
    assert out == [
        ("a", (1, 1)),
        ("b", (2, 1)),
        ("a", (3, 2)),
        ("b", (4, 2)),
        ("a", (5, 3)),
        ("c", (6, 1)),
    ]


def test_flat_map_with_filtering():
    """Test flat_map that expands and filters."""
    out = []

    def expand_and_filter(x):
        # Expand to range, but only keep evens
        return [i for i in range(x) if i % 2 == 0]

    flow = Dataflow("flat_map_filter")
    s = op.input("inp", flow, TestingSource([5, 3, 8]))
    s = op.flat_map("expand", s, expand_and_filter)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    # 5 -> [0, 2, 4], 3 -> [0, 2], 8 -> [0, 2, 4, 6]
    assert out == [0, 2, 4, 0, 2, 0, 2, 4, 6]


def test_chained_filters():
    """Test multiple filters in sequence."""
    out = []

    flow = Dataflow("chained_filters")
    s = op.input("inp", flow, TestingSource(range(1, 21)))
    # Keep only numbers > 5
    s = op.filter("gt_5", s, lambda x: x > 5)
    # Keep only numbers < 15
    s = op.filter("lt_15", s, lambda x: x < 15)
    # Keep only even numbers
    s = op.filter("even", s, lambda x: x % 2 == 0)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [6, 8, 10, 12, 14]


def test_map_with_side_effects_tracking():
    """Test map where we track function calls."""
    out = []
    call_count = []

    def tracked_map(x):
        call_count.append(x)
        return x * 2

    flow = Dataflow("tracked_map")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    s = op.map("track", s, tracked_map)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [2, 4, 6, 8, 10]
    assert call_count == [1, 2, 3, 4, 5]


def test_key_on_with_complex_key_function():
    """Test key_on with complex key extraction."""
    out = []

    @dataclass
    class Record:
        name: str
        category: str
        value: int

    records = [
        Record("item1", "A", 10),
        Record("item2", "B", 20),
        Record("item3", "A", 30),
        Record("item4", "B", 40),
    ]

    flow = Dataflow("complex_key")
    s = op.input("inp", flow, TestingSource(records))
    # Key by category
    keyed = op.key_on("by_category", s, lambda r: r.category)
    # Sum values per category
    values = op.map_value("values", keyed, lambda r: r.value)
    totals = op.reduce_final("sum", values, lambda acc, x: acc + x)
    op.output("out", totals, TestingSink(out))

    run_main(flow)
    result = sorted(out)
    assert result == [("A", 40), ("B", 60)]


def test_branch_with_type_guard_like_behavior():
    """Test branch that separates different types."""
    out_ints = []
    out_strs = []

    flow = Dataflow("type_branch")
    s = op.input("inp", flow, TestingSource([1, "hello", 2, "world", 3]))
    branches = op.branch("type_split", s, lambda x: isinstance(x, int))

    op.output("ints", branches.trues, TestingSink(out_ints))
    op.output("strs", branches.falses, TestingSink(out_strs))

    run_main(flow)
    assert out_ints == [1, 2, 3]
    assert out_strs == ["hello", "world"]


def test_collect_then_process_batches():
    """Test collecting into batches and processing."""
    out = []

    flow = Dataflow("batch_process")
    s = op.input("inp", flow, TestingSource([("key", i) for i in range(10)]))
    # Collect into batches of 3
    batches = op.collect("batch", s, timedelta(seconds=1), max_size=3)
    # Process each batch - sum the numbers
    sums = op.map("sum_batch", batches, lambda kv: (kv[0], sum(kv[1])))
    op.output("out", sums, TestingSink(out))

    run_main(flow)
    # [0,1,2], [3,4,5], [6,7,8], [9]
    assert out == [("key", 3), ("key", 12), ("key", 21), ("key", 9)]


def test_inspect_multiple_points():
    """Test inspecting at multiple points in pipeline."""
    out = []
    inspected_before = []
    inspected_after = []

    flow = Dataflow("multi_inspect")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = op.inspect("before", s, lambda step_id, x: inspected_before.append(x))
    s = op.map("double", s, lambda x: x * 2)
    s = op.inspect("after", s, lambda step_id, x: inspected_after.append(x))
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert inspected_before == [1, 2, 3]
    assert inspected_after == [2, 4, 6]
    assert out == [2, 4, 6]


def test_filter_map_combination():
    """Test filter_map for efficient filtering and mapping."""
    out = []

    def process_evens(x):
        if x % 2 == 0:
            return x * 10
        else:
            return None

    flow = Dataflow("filter_map")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5, 6]))
    s = op.filter_map("proc", s, process_evens)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [20, 40, 60]


def test_nested_dataclass_processing():
    """Test processing nested data structures."""
    out = []

    @dataclass
    class Address:
        city: str
        zipcode: str

    @dataclass
    class Person:
        name: str
        address: Address

    people = [
        Person("Alice", Address("NYC", "10001")),
        Person("Bob", Address("LA", "90001")),
        Person("Charlie", Address("NYC", "10002")),
    ]

    flow = Dataflow("nested")
    s = op.input("inp", flow, TestingSource(people))
    # Key by city
    keyed = op.key_on("by_city", s, lambda p: p.address.city)
    # Extract names
    names = op.map_value("names", keyed, lambda p: p.name)
    # Collect names per city
    grouped = op.fold_final("collect", names, list, lambda acc, x: acc + [x])
    op.output("out", grouped, TestingSink(out))

    run_main(flow)
    result = sorted(out)
    assert result == [("LA", ["Bob"]), ("NYC", ["Alice", "Charlie"])]


def test_max_final_aggregation():
    """Test max_final operator."""
    out = []

    flow = Dataflow("max_agg")
    s = op.input(
        "inp",
        flow,
        TestingSource([("a", 5), ("b", 10), ("a", 15), ("b", 8), ("a", 3)]),
    )
    maxes = op.max_final("max", s)
    op.output("out", maxes, TestingSink(out))

    run_main(flow)
    result = sorted(out)
    assert result == [("a", 15), ("b", 10)]


def test_min_final_aggregation():
    """Test min_final operator."""
    out = []

    flow = Dataflow("min_agg")
    s = op.input(
        "inp",
        flow,
        TestingSource([("a", 5), ("b", 10), ("a", 15), ("b", 8), ("a", 3)]),
    )
    mins = op.min_final("min", s)
    op.output("out", mins, TestingSink(out))

    run_main(flow)
    result = sorted(out)
    assert result == [("a", 3), ("b", 8)]


def test_count_final_aggregation():
    """Test count_final operator."""
    out = []

    words = ["apple", "banana", "apple", "cherry", "banana", "apple"]

    flow = Dataflow("count_agg")
    s = op.input("inp", flow, TestingSource(words))
    counts = op.count_final("count", s, lambda word: word)
    op.output("out", counts, TestingSink(out))

    run_main(flow)
    result = sorted(out)
    assert result == [("apple", 3), ("banana", 2), ("cherry", 1)]
