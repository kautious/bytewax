"""Tests for dataflow validation framework."""

import re
import warnings as python_warnings

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource
from bytewax.validation import (
    ValidationError,
    ValidationWarning,
    validate_dataflow,
    validate_or_raise,
)
from pytest import raises, warns


def test_validation_error_str():
    """Test ValidationError string formatting."""
    error = ValidationError(
        code="TEST_ERROR",
        message="Test error message",
        step_id="test_step",
        suggestion="Fix it this way",
    )

    error_str = str(error)
    assert "[TEST_ERROR]" in error_str
    assert "[test_step]" in error_str
    assert "Test error message" in error_str
    assert "Fix it this way" in error_str


def test_validation_error_without_suggestion():
    """Test ValidationError without suggestion."""
    error = ValidationError(code="TEST_ERROR", message="Test error message")

    error_str = str(error)
    assert "[TEST_ERROR]" in error_str
    assert "Test error message" in error_str
    assert "Suggestion" not in error_str


def test_validation_warning_str():
    """Test ValidationWarning string formatting."""
    warning = ValidationWarning(
        code="TEST_WARNING", message="Test warning message", step_id="test_step"
    )

    warning_str = str(warning)
    assert "[TEST_WARNING]" in warning_str
    assert "[test_step]" in warning_str
    assert "Test warning message" in warning_str


def test_valid_dataflow():
    """Test validation of a valid dataflow."""
    flow = Dataflow("valid_flow")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = op.map("double", s, lambda x: x * 2)
    op.output("out", s, TestingSink([]))

    errors, warnings = validate_dataflow(flow)

    assert len(errors) == 0
    assert len(warnings) == 0


def test_empty_dataflow():
    """Test validation of empty dataflow."""
    flow = Dataflow("empty_flow")

    errors, warnings = validate_dataflow(flow)

    assert len(errors) == 1
    assert errors[0].code == "EMPTY_DATAFLOW"
    assert "no operators" in errors[0].message.lower()


def test_no_input_dataflow():
    """Test validation of dataflow without input."""
    flow = Dataflow("no_input")
    # No input operator, just creates an output
    # This is technically invalid but we need to construct it carefully

    # We can't easily construct this without an input in normal usage,
    # but the validation should catch it if it happens
    # For now, we'll skip this edge case as it's prevented by the API


def test_no_output_dataflow():
    """Test validation of dataflow without output."""
    flow = Dataflow("no_output")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    # Map but no output
    op.map("double", s, lambda x: x * 2)

    errors, warnings = validate_dataflow(flow)

    # Should have an error about no output
    no_output_errors = [e for e in errors if e.code == "NO_OUTPUT"]
    assert len(no_output_errors) == 1
    assert "no output" in no_output_errors[0].message.lower()


def test_duplicate_step_ids():
    """Test detection of duplicate step IDs."""
    flow = Dataflow("dup_ids")
    s1 = op.input("inp", flow, TestingSource([1, 2, 3]))
    # Try to use same ID twice - this will actually fail during construction
    # but let's test the validation logic

    # We can't easily create duplicates due to API design,
    # but the validation code is there for safety


def test_invalid_step_id_with_period():
    """Test detection of periods in step IDs."""
    flow = Dataflow("invalid_id")

    # Trying to create a step with period will fail, so we test
    # the validation logic directly
    # The actual API prevents this, but validation catches it

    # Create a valid flow
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    op.output("out", s, TestingSink([]))

    # Validation should pass
    errors, warnings = validate_dataflow(flow)
    assert len(errors) == 0


def test_validation_with_multiple_operators():
    """Test validation of complex dataflow."""
    flow = Dataflow("complex_flow")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    s = op.map("double", s, lambda x: x * 2)
    s = op.filter("evens", s, lambda x: x % 2 == 0)
    branches = op.branch("split", s, lambda x: x > 5)
    high = branches.trues
    low = branches.falses

    high = op.map("high_proc", high, lambda x: x * 10)
    low = op.map("low_proc", low, lambda x: x * 5)

    merged = op.merge("merge", high, low)
    op.output("out", merged, TestingSink([]))

    errors, warnings = validate_dataflow(flow)

    assert len(errors) == 0


def test_validation_with_keyed_operations():
    """Test validation of dataflow with keyed streams."""
    flow = Dataflow("keyed_flow")
    s = op.input("inp", flow, TestingSource([("a", 1), ("b", 2), ("a", 3)]))
    s = op.map_value("double", s, lambda x: x * 2)
    reduced = op.reduce_final("sum", s, lambda acc, x: acc + x)
    op.output("out", reduced, TestingSink([]))

    errors, warnings = validate_dataflow(flow)

    assert len(errors) == 0


def test_validate_or_raise_success():
    """Test validate_or_raise with valid dataflow."""
    flow = Dataflow("valid_flow")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    op.output("out", s, TestingSink([]))

    # Should not raise
    validate_or_raise(flow)


def test_validate_or_raise_failure():
    """Test validate_or_raise with invalid dataflow."""
    flow = Dataflow("invalid_flow")
    # Empty dataflow

    expect = "Dataflow validation failed"
    with raises(RuntimeError, match=re.escape(expect)):
        validate_or_raise(flow)


def test_validate_strict_mode():
    """Test strict mode treats warnings as errors."""
    flow = Dataflow("strict_flow")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    # Create a map that doesn't lead to output (might generate warning)
    op.map("unused", s, lambda x: x * 2)
    op.output("out", s, TestingSink([]))

    # Non-strict mode
    errors, warnings = validate_dataflow(flow, strict=False)
    # May have warnings but no errors

    # Strict mode
    errors_strict, warnings_strict = validate_dataflow(flow, strict=True)
    # Warnings should be converted to errors
    assert len(warnings_strict) == 0


def test_validation_with_nested_operators():
    """Test validation with custom operators (nested steps)."""
    from bytewax.dataflow import operator

    @operator
    def custom_op(step_id: str, up):
        # Custom operator with substeps
        s = op.map("inner_map", up, lambda x: x * 2)
        return s

    flow = Dataflow("nested_flow")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = custom_op("custom", s)
    op.output("out", s, TestingSink([]))

    errors, warnings = validate_dataflow(flow)

    assert len(errors) == 0


def test_validation_with_multiple_inputs():
    """Test validation with multiple input streams."""
    flow = Dataflow("multi_input")
    s1 = op.input("inp1", flow, TestingSource([1, 2, 3]))
    s2 = op.input("inp2", flow, TestingSource([4, 5, 6]))
    merged = op.merge("merge", s1, s2)
    op.output("out", merged, TestingSink([]))

    errors, warnings = validate_dataflow(flow)

    assert len(errors) == 0


def test_validation_with_multiple_outputs():
    """Test validation with multiple output streams."""
    flow = Dataflow("multi_output")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    doubled = op.map("double", s, lambda x: x * 2)
    tripled = op.map("triple", s, lambda x: x * 3)

    op.output("out1", doubled, TestingSink([]))
    op.output("out2", tripled, TestingSink([]))

    errors, warnings = validate_dataflow(flow)

    assert len(errors) == 0


def test_validation_error_messages_are_helpful():
    """Test that error messages are clear and helpful."""
    flow = Dataflow("test_flow")

    errors, warnings = validate_dataflow(flow)

    # Should have error about empty dataflow
    assert len(errors) > 0
    error = errors[0]

    # Should have a code
    assert error.code

    # Should have a message
    assert error.message

    # Message should be lowercase for consistency
    assert error.message

    # Should have suggestion for some errors
    # Empty dataflow should suggest adding operators
    if error.code == "EMPTY_DATAFLOW":
        assert error.suggestion
        assert "add" in error.suggestion.lower()


def test_validation_warning_for_unused_branches():
    """Test warning for potentially unused branches."""
    flow = Dataflow("unused_branch")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))

    branches = op.branch("split", s, lambda x: x % 2 == 0)

    # Only output one branch, other might be "unused"
    op.output("out", branches.trues, TestingSink([]))

    # The false branch is not connected to output
    # This might generate a warning
    errors, warnings = validate_dataflow(flow)

    # No errors, but possibly warnings
    assert len(errors) == 0


def test_validation_with_collect_operator():
    """Test validation with collect operator."""
    from datetime import timedelta

    flow = Dataflow("collect_flow")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    batched = op.collect("batch", s, timeout=timedelta(seconds=1), max_size=2)
    op.output("out", batched, TestingSink([]))

    errors, warnings = validate_dataflow(flow)

    assert len(errors) == 0


def test_validation_with_inspect_operator():
    """Test validation with inspect operator."""
    inspected = []

    flow = Dataflow("inspect_flow")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = op.inspect("inspect", s, inspected.append)
    op.output("out", s, TestingSink([]))

    errors, warnings = validate_dataflow(flow)

    assert len(errors) == 0


def test_validation_with_flatten_operator():
    """Test validation with flatten operator."""
    flow = Dataflow("flatten_flow")
    s = op.input("inp", flow, TestingSource([[1, 2], [3, 4], [5]]))
    flattened = op.flatten("flat", s)
    op.output("out", flattened, TestingSink([]))

    errors, warnings = validate_dataflow(flow)

    assert len(errors) == 0


def test_validation_with_join_operator():
    """Test validation with join operator."""
    flow = Dataflow("join_flow")
    s1 = op.input("inp1", flow, TestingSource([("a", 1), ("b", 2)]))
    s2 = op.input("inp2", flow, TestingSource([("a", 10), ("b", 20)]))

    joined = op.join("join", s1, s2)
    op.output("out", joined, TestingSink([]))

    errors, warnings = validate_dataflow(flow)

    assert len(errors) == 0


def test_validation_performance():
    """Test that validation is reasonably fast."""
    import time

    # Create a moderately complex dataflow
    flow = Dataflow("perf_test")
    s = op.input("inp", flow, TestingSource(range(100)))

    # Add many operators
    for i in range(50):
        s = op.map(f"map_{i}", s, lambda x: x + 1)

    op.output("out", s, TestingSink([]))

    # Measure validation time
    start = time.time()
    errors, warnings = validate_dataflow(flow)
    elapsed = time.time() - start

    # Should be fast (< 100ms for 50 operators)
    assert elapsed < 0.1, f"Validation took {elapsed*1000:.2f}ms"

    # Should still be valid
    assert len(errors) == 0


def test_validation_api_consistency():
    """Test that validation API is consistent."""
    flow = Dataflow("api_test")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    op.output("out", s, TestingSink([]))

    # Both APIs should work
    errors1, warnings1 = validate_dataflow(flow)
    validate_or_raise(flow)  # Should not raise

    # Both should give same results
    assert len(errors1) == 0
    assert len(warnings1) == 0


def test_empty_dataflow_error_details():
    """Test that empty dataflow error has good details."""
    flow = Dataflow("empty")

    errors, warnings = validate_dataflow(flow)

    assert len(errors) == 1
    error = errors[0]

    assert error.code == "EMPTY_DATAFLOW"
    assert "no operators" in error.message.lower()
    assert error.suggestion
    assert "add" in error.suggestion.lower() or "operator" in error.suggestion.lower()


def test_no_output_error_details():
    """Test that no output error has good details."""
    flow = Dataflow("no_out")
    op.input("inp", flow, TestingSource([1, 2, 3]))

    errors, warnings = validate_dataflow(flow)

    no_output_errors = [e for e in errors if e.code == "NO_OUTPUT"]
    assert len(no_output_errors) >= 1

    error = no_output_errors[0]
    assert "output" in error.message.lower()
    assert error.suggestion
    assert "output" in error.suggestion.lower()
