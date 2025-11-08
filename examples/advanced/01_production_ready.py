"""Production-Ready Dataflow - Using All Phase 1 & 2 Features

This comprehensive example demonstrates a production-ready dataflow using:
- Phase 1: Validation, enhanced errors, operator discovery
- Phase 2: Debugging utilities, fluent API, helper operators

This example shows best practices for building reliable, debuggable,
and maintainable dataflows.

Concepts demonstrated:
- Pre-flight validation
- Debugging with sampling
- Performance profiling
- Fluent API for cleaner code
- Helper operators for common patterns
- Error handling with context
- Comprehensive logging
"""

from datetime import datetime
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource, TestingSink, run_main

# Phase 1 imports
from bytewax.validation import validate_dataflow, validate_or_raise
from bytewax.errors import OperatorError, DataflowError

# Phase 2 imports
from bytewax.debug import StreamSampler, profile_function, StreamCounter
from bytewax.fluent import add_fluent_methods
from bytewax.operators.helpers import deduplicate, sample, take, default_value


def example_1_with_validation():
    """Example 1: Validate dataflow before running."""
    print("\n" + "=" * 60)
    print("Example 1: Dataflow Validation")
    print("=" * 60)

    # Create a dataflow
    flow = Dataflow("validated_flow")
    s = op.input("data", flow, TestingSource([1, 2, 3, 4, 5]))
    s = op.map("double", s, lambda x: x * 2)
    op.output("out", s, TestingSink([]))

    # Validate before running
    print("\n✓ Validating dataflow...")
    errors, warnings = validate_dataflow(flow)

    if errors:
        print("❌ Validation errors found:")
        for err in errors:
            print(f"  - {err}")
    else:
        print("✅ Dataflow is valid!")

    if warnings:
        print("⚠️  Warnings:")
        for warn in warnings:
            print(f"  - {warn}")

    # Alternative: raise on error
    try:
        validate_or_raise(flow)
        print("✓ validate_or_raise passed")
    except RuntimeError as e:
        print(f"❌ Validation failed: {e}")


def example_2_with_debugging():
    """Example 2: Debug with sampling and profiling."""
    print("\n" + "=" * 60)
    print("Example 2: Debugging with Sampling")
    print("=" * 60)

    flow = Dataflow("debug_flow")

    # Create sampler to capture stream samples
    sampler = StreamSampler(max_samples=5)
    counter = StreamCounter()

    # Generate some data
    data = list(range(1, 101))
    s = op.input("data", flow, TestingSource(data))

    # Attach sampler after input
    s = sampler.attach("after_input", s)
    s = counter.attach("input_count", s)

    # Transform
    s = op.map("double", s, lambda x: x * 2)
    s = sampler.attach("after_double", s)

    # Filter
    s = op.filter("gt_100", s, lambda x: x > 100)
    s = sampler.attach("after_filter", s)
    s = counter.attach("output_count", s)

    op.output("out", s, TestingSink([]))

    run_main(flow)

    # Check samples
    print("\nSampled data after input:")
    for sample in sampler.get_samples("after_input"):
        print(f"  {sample}")

    print("\nSampled data after filter:")
    for sample in sampler.get_samples("after_filter"):
        print(f"  {sample}")

    # Check counts
    print(f"\nItems processed: {counter.get_count('input_count')}")
    print(f"Items after filter: {counter.get_count('output_count')}")

    # Get summary
    print("\nSampling summary:")
    print(sampler.summary())


def example_3_fluent_api():
    """Example 3: Clean code with fluent API."""
    print("\n" + "=" * 60)
    print("Example 3: Fluent API Pattern")
    print("=" * 60)

    # Enable fluent methods
    add_fluent_methods()

    flow = Dataflow("fluent_flow")

    # Build pipeline with method chaining
    result = []
    (
        op.input("data", flow, TestingSource(range(1, 21)))
        .filter("odd", lambda x: x % 2 == 1)
        .map("square", lambda x: x**2)
        .filter("gt_25", lambda x: x > 25)
        .inspect("debug", lambda x: None)  # Could print here
        .output("out", TestingSink(result))
    )

    run_main(flow)

    print("\nOdd numbers from 1-20, squared, where result > 25:")
    print(f"  {sorted(result)}")


def example_4_helper_operators():
    """Example 4: Using helper operators."""
    print("\n" + "=" * 60)
    print("Example 4: Helper Operators")
    print("=" * 60)

    flow = Dataflow("helpers_flow")

    # Data with duplicates and None values
    data = [1, 2, 2, None, 3, 4, 4, None, 5, 5, 5, 6, 7, 8, 9, 10]
    s = op.input("data", flow, TestingSource(data))

    # Use helpers: deduplicate, fill None, sample, take
    s = deduplicate("dedup", s)
    s = default_value("fill_none", s, default=0)
    s = take("first_5", s, n=5)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print(f"\nOriginal: {data}")
    print(f"After dedup + fill_none + take(5): {output}")


def example_5_comprehensive_pipeline():
    """Example 5: Comprehensive production-ready pipeline."""
    print("\n" + "=" * 60)
    print("Example 5: Production-Ready Pipeline")
    print("=" * 60)

    # Simulated real-world data: user events
    events = [
        {"user_id": "alice", "action": "view", "value": 10, "timestamp": "2024-01-01T10:00:00"},
        {"user_id": "bob", "action": "view", "value": 15, "timestamp": "2024-01-01T10:01:00"},
        {"user_id": "alice", "action": "click", "value": 20, "timestamp": "2024-01-01T10:02:00"},
        {"user_id": "alice", "action": "view", "value": 10, "timestamp": "2024-01-01T10:00:00"},  # Duplicate
        {"user_id": "charlie", "action": "purchase", "value": 100, "timestamp": "2024-01-01T10:03:00"},
        {"user_id": "bob", "action": "purchase", "value": 50, "timestamp": "2024-01-01T10:04:00"},
    ] * 5  # Repeat for more data

    flow = Dataflow("production_pipeline")

    # Phase 1: Validate before running
    validate_or_raise(flow)

    # Phase 2: Set up debugging
    sampler = StreamSampler(max_samples=10)
    counter = StreamCounter()

    # Build pipeline
    s = op.input("events", flow, TestingSource(events))
    s = counter.attach("total_events", s)

    # Deduplicate events (based on entire event)
    s = deduplicate("dedup", s, key_fn=lambda e: str(e))
    s = counter.attach("after_dedup", s)

    # Extract user_id and action
    s = op.map("extract", s, lambda e: (e["user_id"], e))
    s = sampler.attach("keyed_events", s)

    # Filter: only purchases
    s = op.filter("purchases", s, lambda pair: pair[1]["action"] == "purchase")
    s = counter.attach("purchases_only", s)

    # Aggregate: total purchase value per user
    s = op.map("get_value", s, lambda pair: (pair[0], pair[1]["value"]))
    s = op.reduce_final("sum_values", s, lambda acc, x: acc + x)

    s = sampler.attach("final_aggregates", s)

    output = []
    op.output("out", s, TestingSink(output))

    print("\n🚀 Running production pipeline...")
    run_main(flow)

    # Results
    print("\n📊 Results:")
    print(f"  Total events processed: {counter.get_count('total_events')}")
    print(f"  After deduplication: {counter.get_count('after_dedup')}")
    print(f"  Purchase events: {counter.get_count('purchases_only')}")

    print("\n💰 Purchase totals by user:")
    for user, total in sorted(output):
        print(f"  {user}: ${total}")

    print("\n🔍 Sample of keyed events:")
    for sample in sampler.get_samples("keyed_events")[:3]:
        print(f"  {sample}")


def example_6_error_handling_with_context():
    """Example 6: Enhanced error handling with OperatorError."""
    print("\n" + "=" * 60)
    print("Example 6: Enhanced Error Messages")
    print("=" * 60)

    flow = Dataflow("error_context_flow")

    data = ["10", "20", "invalid", "30"]
    s = op.input("data", flow, TestingSource(data))

    def parse_with_context(x):
        """Parse with enhanced error reporting."""
        try:
            return int(x)
        except ValueError:
            # Use OperatorError for better context
            raise OperatorError(
                step_id="parse",
                operator_name="map",
                message=f"Failed to parse value: '{x}'",
                suggestion="Ensure all input values are valid integers",
            )

    # Note: This will raise an error, so we catch it
    try:
        s = op.map("parse", s, parse_with_context)
        output = []
        op.output("out", s, TestingSink(output))
        run_main(flow)
    except OperatorError as e:
        print("\n❌ Caught OperatorError:")
        print(f"  Step: {e.step_id}")
        print(f"  Operator: {e.operator_name}")
        print(f"  Message: {e.message}")
        if e.suggestion:
            print(f"  💡 Suggestion: {e.suggestion}")


def example_7_operator_discovery():
    """Example 7: Discover operators programmatically."""
    print("\n" + "=" * 60)
    print("Example 7: Operator Discovery")
    print("=" * 60)

    from bytewax.discovery import (
        list_operators,
        describe_operator,
        search_operators,
    )

    # List all stateful operators
    print("\nStateful operators:")
    stateful = list_operators(category="stateful")
    for name, summary in stateful[:5]:
        print(f"  {name:20s} - {summary}")

    # Search for window operators
    print("\nWindow-related operators:")
    window_ops = search_operators("window")
    for name, summary in window_ops[:5]:
        print(f"  {name:20s} - {summary}")

    # Get detailed info about map
    print("\nDetailed info about 'map':")
    info = describe_operator("map")
    print(f"  Category: {info.category}")
    print(f"  Summary: {info.summary}")
    print(f"  Signature: {info.signature}")


if __name__ == "__main__":
    print("\n🏭 Production-Ready Dataflow Examples")
    print("Using all Phase 1 & 2 features")
    print("=" * 60)

    example_1_with_validation()
    example_2_with_debugging()
    example_3_fluent_api()
    example_4_helper_operators()
    example_5_comprehensive_pipeline()
    example_6_error_handling_with_context()
    example_7_operator_discovery()

    print("\n" + "=" * 60)
    print("✅ All production examples completed!")
    print("=" * 60)


"""
Key Takeaways - Production Best Practices:

1. VALIDATE EARLY:
   - Use validate_dataflow() before running
   - Catch configuration errors early
   - Use validate_or_raise() in production

2. DEBUG EFFECTIVELY:
   - StreamSampler: capture samples for inspection
   - StreamCounter: track item counts
   - profile_function: find performance bottlenecks
   - Non-intrusive debugging

3. WRITE CLEAN CODE:
   - Fluent API for readable pipelines
   - Method chaining reduces boilerplate
   - Self-documenting dataflows

4. USE HELPERS:
   - deduplicate: remove duplicates
   - sample: test with subset
   - take: limit items for testing
   - default_value: handle None values

5. HANDLE ERRORS:
   - OperatorError for context
   - Suggestions for fixes
   - Links to documentation

6. DISCOVER OPERATORS:
   - list_operators(): browse all operators
   - search_operators(): find by keyword
   - describe_operator(): detailed info

7. MONITOR METRICS:
   - Track counts at key points
   - Sample data for debugging
   - Profile performance
   - Log errors with context

8. PRODUCTION CHECKLIST:
   ✓ Validate dataflow structure
   ✓ Handle errors gracefully
   ✓ Add monitoring/sampling
   ✓ Document operator purpose
   ✓ Test with sample data
   ✓ Profile for bottlenecks
   ✓ Log comprehensively

This example demonstrates the full power of Bytewax with
Phase 1 & 2 improvements for building production-ready,
debuggable, and maintainable stream processing pipelines!
"""
