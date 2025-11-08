"""Comprehensive Integration Tests - All Phases

This test file verifies that all Phase 1, 2, and 3 features work together correctly.
Tests the integration between validation, debugging, fluent API, and helpers.
"""

import pytest
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource, TestingSink, run_main

# Phase 1 imports
from bytewax.validation import validate_dataflow, validate_or_raise, ValidationError
from bytewax.errors import OperatorError, DataflowError
from bytewax.discovery import list_operators, describe_operator, search_operators

# Phase 2 imports
from bytewax.debug import StreamSampler, profile_function, StreamCounter
from bytewax.fluent import add_fluent_methods, remove_fluent_methods, FluentStream
from bytewax.operators.helpers import deduplicate, sample, take, default_value, tee


def test_validation_with_debugging():
    """Test that validation works with debugging utilities."""
    flow = Dataflow("validation_debug_test")

    # Set up debugging
    sampler = StreamSampler(max_samples=10)
    counter = StreamCounter()

    # Build dataflow
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    s = sampler.attach("input", s)
    s = counter.attach("input_count", s)
    s = op.map("double", s, lambda x: x * 2)
    op.output("out", s, TestingSink([]))

    # Validate
    errors, warnings = validate_dataflow(flow)
    assert len(errors) == 0, "Should have no errors"

    # Run
    run_main(flow)

    # Verify debugging worked
    assert len(sampler.get_samples("input")) > 0
    assert counter.get_count("input_count") == 5


def test_fluent_api_with_validation():
    """Test that fluent API dataflows can be validated."""
    add_fluent_methods()

    try:
        flow = Dataflow("fluent_validation_test")

        output = []
        (
            op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
            .map("double", lambda x: x * 2)
            .filter("positive", lambda x: x > 0)
            .output("out", TestingSink(output))
        )

        # Validate
        errors, warnings = validate_dataflow(flow)
        assert len(errors) == 0

        # Run
        run_main(flow)
        assert len(output) == 5

    finally:
        remove_fluent_methods()


def test_helpers_with_debugging():
    """Test that helper operators work with debugging."""
    flow = Dataflow("helpers_debug_test")

    sampler = StreamSampler(max_samples=100)
    counter = StreamCounter()

    # Use helpers with debugging
    s = op.input("inp", flow, TestingSource([1, 2, 2, 3, 3, 3, 4, 5, 5]))
    s = counter.attach("before_dedup", s)

    s = deduplicate("dedup", s)
    s = sampler.attach("after_dedup", s)
    s = counter.attach("after_dedup_count", s)

    s = default_value("fill", s, default=0)
    s = take("limit", s, n=3)
    s = counter.attach("final", s)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    # Verify debugging
    assert counter.get_count("before_dedup") == 9
    assert counter.get_count("after_dedup_count") == 5  # Unique values
    assert counter.get_count("final") == 3  # After take
    assert len(output) == 3


def test_validation_error_handling():
    """Test validation of intentionally broken dataflows."""
    # No input
    flow1 = Dataflow("no_input")
    s = op.map("transform", None, lambda x: x)  # Invalid

    # This should not raise during construction
    # Validation should catch it

    # No output
    flow2 = Dataflow("no_output")
    s = op.input("inp", flow2, TestingSource([1, 2, 3]))
    s = op.map("transform", s, lambda x: x * 2)
    # Missing output

    errors, _ = validate_dataflow(flow2)
    assert len(errors) > 0
    assert any("output" in str(e).lower() for e in errors)


def test_profiling_with_fluent_api():
    """Test that profiling works with fluent API."""
    add_fluent_methods()

    try:
        @profile_function("expensive_op")
        def expensive_transform(x):
            return x ** 2

        flow = Dataflow("profile_fluent_test")
        output = []

        (
            op.input("inp", flow, TestingSource(range(10)))
            .map("expensive", expensive_transform)
            .output("out", TestingSink(output))
        )

        run_main(flow)

        # Verify profiling recorded stats
        stats = expensive_transform.stats
        assert stats.call_count == 10
        assert stats.avg_time_ms >= 0

    finally:
        remove_fluent_methods()


def test_discovery_finds_new_helpers():
    """Test that operator discovery can find new helper operators."""
    # Search for new helpers
    results = search_operators("deduplicate")
    assert len(results) > 0

    results = search_operators("sample")
    assert len(results) > 0

    # List all operators should include helpers
    all_ops = list_operators()
    op_names = [name for name, _ in all_ops]

    # Check that standard operators are present
    assert any("map" in name for name in op_names)
    assert any("filter" in name for name in op_names)


def test_complex_pipeline_integration():
    """Test a complex pipeline using all features."""
    flow = Dataflow("complex_integration_test")

    # Phase 2 setup
    sampler = StreamSampler(max_samples=50)
    counter = StreamCounter()

    # Sample data with duplicates and None values
    data = [
        {"id": "1", "value": 10},
        {"id": "2", "value": 20},
        {"id": "1", "value": 10},  # Duplicate
        {"id": "3", "value": None},
        {"id": "4", "value": 40},
        {"id": "2", "value": 20},  # Duplicate
        {"id": "5", "value": 50},
    ]

    s = op.input("events", flow, TestingSource(data))
    s = counter.attach("total", s)
    s = sampler.attach("raw_input", s)

    # Deduplicate by ID
    s = deduplicate("dedup", s, key_fn=lambda x: x["id"])
    s = counter.attach("after_dedup", s)

    # Handle None values
    s = op.map("get_value", s, lambda x: x.get("value"))
    s = default_value("fill_none", s, default=0)
    s = sampler.attach("after_fill", s)

    # Filter and limit
    s = op.filter("positive", s, lambda x: x > 0)
    s = counter.attach("after_filter", s)

    output = []
    op.output("out", s, TestingSink(output))

    # Phase 1: Validate
    errors, warnings = validate_dataflow(flow)
    assert len(errors) == 0

    # Run
    run_main(flow)

    # Verify results
    assert counter.get_count("total") == 7
    assert counter.get_count("after_dedup") == 5  # Unique IDs
    assert counter.get_count("after_filter") == 4  # Positive values only

    # Verify sampling worked
    assert len(sampler.get_samples("raw_input")) > 0
    assert len(sampler.get_samples("after_fill")) > 0


def test_error_context_in_operators():
    """Test that OperatorError provides helpful context."""
    flow = Dataflow("error_context_test")

    def failing_mapper(x):
        if x == 3:
            raise OperatorError(
                step_id="transform",
                operator_name="map",
                message="Failed to process value 3",
                suggestion="Check input data quality",
            )
        return x * 2

    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    s = op.map("transform", s, failing_mapper)
    op.output("out", s, TestingSink([]))

    # Should raise OperatorError with context
    with pytest.raises(OperatorError) as exc_info:
        run_main(flow)

    error = exc_info.value
    assert error.step_id == "transform"
    assert error.operator_name == "map"
    assert "value 3" in error.message
    assert error.suggestion is not None


def test_tee_with_separate_processing():
    """Test tee helper with different processing branches."""
    flow = Dataflow("tee_integration_test")

    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))

    # Tee the stream
    s1, s2 = tee("split", s)

    # Branch 1: evens
    evens = op.filter("evens", s1, lambda x: x % 2 == 0)
    evens = op.map("double_evens", evens, lambda x: x * 2)

    # Branch 2: odds
    odds = op.filter("odds", s2, lambda x: x % 2 == 1)
    odds = op.map("triple_odds", odds, lambda x: x * 3)

    even_output = []
    odd_output = []

    op.output("evens_out", evens, TestingSink(even_output))
    op.output("odds_out", odds, TestingSink(odd_output))

    # Validate
    errors, warnings = validate_dataflow(flow)
    assert len(errors) == 0

    run_main(flow)

    # Verify results
    assert len(even_output) == 5  # 2, 4, 6, 8, 10
    assert len(odd_output) == 5   # 1, 3, 5, 7, 9
    assert even_output == [4, 8, 12, 16, 20]  # Doubled
    assert odd_output == [3, 9, 15, 21, 27]   # Tripled


def test_fluent_stream_wrapper():
    """Test FluentStream wrapper without modifying Stream class."""
    flow = Dataflow("fluent_wrapper_test")

    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))

    # Use FluentStream wrapper
    fluent = FluentStream(s)
    result = (
        fluent
        .map("double", lambda x: x * 2)
        .filter("gt_5", lambda x: x > 5)
        .map("add_10", lambda x: x + 10)
        .unwrap()  # Get back Stream
    )

    output = []
    op.output("out", result, TestingSink(output))

    run_main(flow)

    # 1,2,3,4,5 -> 2,4,6,8,10 -> 6,8,10 -> 16,18,20
    assert output == [16, 18, 20]


def test_sampling_for_debugging():
    """Test using sample helper for debugging large datasets."""
    flow = Dataflow("sampling_debug_test")

    # Large dataset
    large_data = list(range(1000))

    s = op.input("inp", flow, TestingSource(large_data))

    # Sample for debugging (10%)
    s = sample("debug_sample", s, rate=0.1, seed=42)

    # Continue processing sample
    s = op.map("transform", s, lambda x: x * 2)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    # Should process roughly 100 items (10% of 1000)
    assert 50 < len(output) < 150


def test_validate_or_raise_integration():
    """Test validate_or_raise in production-like scenario."""
    flow = Dataflow("validate_raise_test")

    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = op.map("double", s, lambda x: x * 2)
    op.output("out", s, TestingSink([]))

    # Should not raise
    validate_or_raise(flow)

    # Now test with invalid dataflow
    bad_flow = Dataflow("bad_flow")
    s = op.input("inp", bad_flow, TestingSource([1, 2, 3]))
    # Missing output

    with pytest.raises(RuntimeError) as exc_info:
        validate_or_raise(bad_flow)

    assert "validation" in str(exc_info.value).lower()


def test_all_helpers_composition():
    """Test composing all helper operators together."""
    flow = Dataflow("all_helpers_test")

    # Create data with various issues
    data = [1, 2, 2, None, 3, 4, 4, 5, None, 6, 7, 8, 9, 10] * 2  # 28 items

    s = op.input("inp", flow, TestingSource(data))

    # Compose all helpers
    s = deduplicate("dedup", s)          # Remove duplicates
    s = default_value("fill", s, default=0)  # Replace None
    s = op.filter("positive", s, lambda x: x > 0)  # Filter zeros
    s = sample("sample", s, rate=1.0, seed=42)  # Keep all (for testing)
    s = take("limit", s, n=5)            # Take first 5

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    # Should have at most 5 unique positive values
    assert len(output) <= 5
    assert all(x > 0 for x in output)
    assert len(set(output)) == len(output)  # All unique


def test_discovery_describe_operators():
    """Test describing operators for documentation."""
    # Describe a core operator
    info = describe_operator("map")
    assert info.name == "map"
    assert info.category is not None
    assert len(info.summary) > 0

    # Describe a stateful operator
    info = describe_operator("stateful_map")
    assert info.name == "stateful_map"
    assert "state" in info.summary.lower() or "state" in info.category.lower()


def test_stream_counter_multiple_checkpoints():
    """Test StreamCounter with multiple measurement points."""
    flow = Dataflow("counter_checkpoints_test")

    counter = StreamCounter()

    s = op.input("inp", flow, TestingSource(range(100)))
    s = counter.attach("start", s)

    s = op.filter("even", s, lambda x: x % 2 == 0)
    s = counter.attach("after_filter", s)

    s = op.map("double", s, lambda x: x * 2)
    s = counter.attach("after_double", s)

    s = op.filter("gt_50", s, lambda x: x > 50)
    s = counter.attach("final", s)

    op.output("out", s, TestingSink([]))

    run_main(flow)

    # Verify counts at each checkpoint
    assert counter.get_count("start") == 100
    assert counter.get_count("after_filter") == 50  # Even numbers
    assert counter.get_count("after_double") == 50  # Still 50
    assert counter.get_count("final") < 50  # Filtered by > 50


if __name__ == "__main__":
    # Run tests manually
    print("Running comprehensive integration tests...")

    test_validation_with_debugging()
    print("✓ test_validation_with_debugging")

    test_fluent_api_with_validation()
    print("✓ test_fluent_api_with_validation")

    test_helpers_with_debugging()
    print("✓ test_helpers_with_debugging")

    test_validation_error_handling()
    print("✓ test_validation_error_handling")

    test_profiling_with_fluent_api()
    print("✓ test_profiling_with_fluent_api")

    test_discovery_finds_new_helpers()
    print("✓ test_discovery_finds_new_helpers")

    test_complex_pipeline_integration()
    print("✓ test_complex_pipeline_integration")

    test_error_context_in_operators()
    print("✓ test_error_context_in_operators")

    test_tee_with_separate_processing()
    print("✓ test_tee_with_separate_processing")

    test_fluent_stream_wrapper()
    print("✓ test_fluent_stream_wrapper")

    test_sampling_for_debugging()
    print("✓ test_sampling_for_debugging")

    test_validate_or_raise_integration()
    print("✓ test_validate_or_raise_integration")

    test_all_helpers_composition()
    print("✓ test_all_helpers_composition")

    test_discovery_describe_operators()
    print("✓ test_discovery_describe_operators")

    test_stream_counter_multiple_checkpoints()
    print("✓ test_stream_counter_multiple_checkpoints")

    print("\n✅ All integration tests passed!")
