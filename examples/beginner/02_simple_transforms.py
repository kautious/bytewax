"""Simple Transformations - Map, Filter, and FlatMap

This example demonstrates the three most common transformation operators:
- map: Transform each item 1-to-1
- filter: Keep only items that match a condition
- flat_map: Transform each item to 0 or more items

Concepts introduced:
- Map operator for transformations
- Filter operator for conditional processing
- FlatMap operator for one-to-many transformations
- Inspect operator for debugging
"""

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource, TestingSink, run_main


def example_1_map():
    """Example 1: Using map to transform data."""
    print("\n" + "=" * 60)
    print("Example 1: MAP - Transform Each Item")
    print("=" * 60)

    flow = Dataflow("map_example")

    # Input: numbers 1 through 10
    s = op.input("numbers", flow, TestingSource(list(range(1, 11))))

    # Transform: square each number
    s = op.map("square", s, lambda x: x**2)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("Original: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]")
    print(f"Squared:  {output}")


def example_2_filter():
    """Example 2: Using filter to select data."""
    print("\n" + "=" * 60)
    print("Example 2: FILTER - Keep Only Matching Items")
    print("=" * 60)

    flow = Dataflow("filter_example")

    s = op.input("numbers", flow, TestingSource(list(range(1, 21))))

    # Keep only even numbers
    s = op.filter("even", s, lambda x: x % 2 == 0)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("Original: [1, 2, 3, ..., 20]")
    print(f"Even:     {output}")


def example_3_filter_and_map():
    """Example 3: Combining filter and map."""
    print("\n" + "=" * 60)
    print("Example 3: FILTER + MAP - Chain Operations")
    print("=" * 60)

    flow = Dataflow("filter_map_example")

    s = op.input("numbers", flow, TestingSource(list(range(1, 11))))

    # Filter: keep only odd numbers
    s = op.filter("odd", s, lambda x: x % 2 == 1)

    # Map: triple them
    s = op.map("triple", s, lambda x: x * 3)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("Original:      [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]")
    print("Odd:           [1, 3, 5, 7, 9]")
    print(f"Tripled:       {output}")


def example_4_flat_map():
    """Example 4: Using flat_map for one-to-many transformations."""
    print("\n" + "=" * 60)
    print("Example 4: FLAT_MAP - One Item to Many Items")
    print("=" * 60)

    flow = Dataflow("flat_map_example")

    # Input: sentences
    sentences = [
        "Hello World",
        "Bytewax is awesome",
        "Stream processing rocks",
    ]
    s = op.input("sentences", flow, TestingSource(sentences))

    # Split each sentence into words
    s = op.flat_map("split", s, lambda sentence: sentence.split())

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("Original sentences:")
    for sentence in sentences:
        print(f"  - {sentence}")
    print(f"\nWords: {output}")
    print(f"Total words: {len(output)}")


def example_5_inspect():
    """Example 5: Using inspect to debug data flow."""
    print("\n" + "=" * 60)
    print("Example 5: INSPECT - Debug Your Data Flow")
    print("=" * 60)

    flow = Dataflow("inspect_example")

    s = op.input("numbers", flow, TestingSource([1, 2, 3, 4, 5]))

    # Inspect after input
    s = op.inspect("after_input", s, lambda x: print(f"  Input: {x}"))

    # Transform
    s = op.map("double", s, lambda x: x * 2)

    # Inspect after transformation
    s = op.inspect("after_double", s, lambda x: print(f"  Doubled: {x}"))

    # Filter
    s = op.filter("gt_5", s, lambda x: x > 5)

    # Inspect after filter
    s = op.inspect("after_filter", s, lambda x: print(f"  Filtered: {x}"))

    output = []
    op.output("out", s, TestingSink(output))

    print("\nData flow trace:")
    run_main(flow)

    print(f"\nFinal result: {output}")


def example_6_real_world():
    """Example 6: Real-world data processing pipeline."""
    print("\n" + "=" * 60)
    print("Example 6: REAL-WORLD - Process User Events")
    print("=" * 60)

    flow = Dataflow("user_events")

    # Input: user events
    events = [
        {"user": "alice", "action": "login", "value": 1},
        {"user": "bob", "action": "click", "value": 5},
        {"user": "alice", "action": "purchase", "value": 100},
        {"user": "charlie", "action": "login", "value": 1},
        {"user": "bob", "action": "purchase", "value": 50},
        {"user": "alice", "action": "click", "value": 3},
    ]
    s = op.input("events", flow, TestingSource(events))

    # Filter: keep only purchases
    s = op.filter("purchases_only", s, lambda e: e["action"] == "purchase")

    # Map: extract purchase amounts
    s = op.map("get_amount", s, lambda e: e["value"])

    # Filter: keep only high-value purchases (>= 75)
    s = op.filter("high_value", s, lambda amount: amount >= 75)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print(f"Total events: {len(events)}")
    print(f"High-value purchases: {output}")
    print(f"Total revenue (high-value): ${sum(output)}")


if __name__ == "__main__":
    print("\n🎯 Simple Transformations Examples")
    print("Learn the fundamental operators: map, filter, flat_map")

    # Run all examples
    example_1_map()
    example_2_filter()
    example_3_filter_and_map()
    example_4_flat_map()
    example_5_inspect()
    example_6_real_world()

    print("\n" + "=" * 60)
    print("✅ All examples completed!")
    print("=" * 60)


"""
Key Takeaways:

1. MAP operator:
   - Transforms each input item to exactly one output item
   - Signature: op.map(step_id, stream, function)
   - Use for: conversions, calculations, formatting

2. FILTER operator:
   - Keeps only items that match a condition
   - Signature: op.filter(step_id, stream, predicate)
   - Use for: filtering, validation, selection

3. FLAT_MAP operator:
   - Transforms each input to 0 or more outputs
   - Signature: op.flat_map(step_id, stream, function)
   - Use for: splitting, expanding, parsing

4. INSPECT operator:
   - Useful for debugging - see data as it flows
   - Doesn't modify the stream
   - Signature: op.inspect(step_id, stream, inspector)

5. Operators are composable:
   - Chain them together to build complex pipelines
   - Each operator returns a new stream
   - Order matters!

Try modifying these examples:
- Change the transformation functions
- Add more operators to the pipeline
- Create your own data and process it
"""
