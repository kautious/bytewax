"""Tests for error handling in errors.py."""

import re

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.errors import (
    BytewaxRuntimeError,
    ConfigurationError,
    DataflowError,
    OperatorError,
)
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import raises


def test_bytewax_runtime_error_is_runtime_error():
    """Test that BytewaxRuntimeError is a subclass of RuntimeError."""
    assert issubclass(BytewaxRuntimeError, RuntimeError)


def test_bytewax_runtime_error_can_be_raised():
    """Test that BytewaxRuntimeError can be raised."""
    with raises(BytewaxRuntimeError):
        raise BytewaxRuntimeError("test error")


def test_bytewax_runtime_error_with_message():
    """Test BytewaxRuntimeError with custom message."""
    msg = "Custom error message"
    with raises(BytewaxRuntimeError, match=re.escape(msg)):
        raise BytewaxRuntimeError(msg)


def test_bytewax_runtime_error_chaining():
    """Test that BytewaxRuntimeError can be chained from other exceptions."""
    try:
        try:
            raise ValueError("Original error")
        except ValueError as e:
            raise BytewaxRuntimeError("Runtime error") from e
    except BytewaxRuntimeError as runtime_err:
        assert runtime_err.__cause__ is not None
        assert isinstance(runtime_err.__cause__, ValueError)
        assert str(runtime_err.__cause__) == "Original error"


def test_bytewax_runtime_error_in_operator():
    """Test BytewaxRuntimeError propagation from operators."""
    out = []

    def raise_runtime_error(x):
        if x == 2:
            raise BytewaxRuntimeError("Error at item 2")
        return x

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = op.map("raise_error", s, raise_runtime_error)
    op.output("out", s, TestingSink(out))

    # Bytewax wraps errors with its own message mentioning the operator step
    expect = "error calling.*mapper.*raise_error"
    with raises(BytewaxRuntimeError, match=expect):
        run_main(flow)


def test_bytewax_runtime_error_with_context():
    """Test BytewaxRuntimeError with additional context."""
    try:
        try:
            # Simulate some operation that fails
            x = 1 / 0
        except ZeroDivisionError as e:
            raise BytewaxRuntimeError("Division failed in operator") from e
    except BytewaxRuntimeError as err:
        assert "Division failed in operator" in str(err)
        assert isinstance(err.__cause__, ZeroDivisionError)


def test_bytewax_runtime_error_inheritance():
    """Test that catching RuntimeError also catches BytewaxRuntimeError."""
    try:
        raise BytewaxRuntimeError("Test")
    except RuntimeError as e:
        # Should be caught as RuntimeError
        assert isinstance(e, BytewaxRuntimeError)
        assert isinstance(e, RuntimeError)


def test_operator_error_basic():
    """Test OperatorError creation and formatting."""
    error = OperatorError(
        step_id="my_flow.transform",
        operator_name="map",
        message="Failed to process item",
    )

    error_str = str(error)
    assert "[map:my_flow.transform]" in error_str
    assert "Failed to process item" in error_str

    assert error.step_id == "my_flow.transform"
    assert error.operator_name == "map"


def test_operator_error_with_suggestion():
    """Test OperatorError with suggestion."""
    error = OperatorError(
        step_id="my_flow.filter",
        operator_name="filter",
        message="Predicate returned non-boolean",
        suggestion="Ensure your filter function returns True or False",
    )

    error_str = str(error)
    assert "Suggestion:" in error_str
    assert "returns True or False" in error_str

    assert error.suggestion is not None


def test_operator_error_with_docs_url():
    """Test OperatorError with documentation URL."""
    error = OperatorError(
        step_id="my_flow.map",
        operator_name="map",
        message="Invalid mapper",
        docs_url="https://docs.bytewax.io/operators/map",
    )

    error_str = str(error)
    assert "Documentation:" in error_str
    assert "https://docs.bytewax.io/operators/map" in error_str

    assert error.docs_url is not None


def test_operator_error_full():
    """Test OperatorError with all fields."""
    error = OperatorError(
        step_id="flow.step",
        operator_name="reduce",
        message="Reducer function failed",
        suggestion="Check that your reducer handles None values",
        docs_url="https://docs.bytewax.io/operators/reduce",
    )

    error_str = str(error)
    assert "[reduce:flow.step]" in error_str
    assert "Reducer function failed" in error_str
    assert "Suggestion:" in error_str
    assert "Documentation:" in error_str


def test_operator_error_inheritance():
    """Test that OperatorError is a BytewaxRuntimeError."""
    error = OperatorError(
        step_id="test.op", operator_name="test", message="Test error"
    )

    assert isinstance(error, OperatorError)
    assert isinstance(error, BytewaxRuntimeError)
    assert isinstance(error, RuntimeError)


def test_operator_error_can_be_raised():
    """Test that OperatorError can be raised and caught."""
    with raises(OperatorError) as exc_info:
        raise OperatorError(
            step_id="test.op", operator_name="map", message="Test error"
        )

    assert exc_info.value.step_id == "test.op"
    assert exc_info.value.operator_name == "map"


def test_dataflow_error_basic():
    """Test DataflowError creation and formatting."""
    error = DataflowError("Dataflow has no input operators")

    error_str = str(error)
    assert "no input operators" in error_str


def test_dataflow_error_with_suggestion():
    """Test DataflowError with suggestion."""
    error = DataflowError(
        "Dataflow has duplicate step IDs",
        suggestion="Ensure each step has a unique ID",
    )

    error_str = str(error)
    assert "duplicate step IDs" in error_str
    assert "Suggestion:" in error_str
    assert "unique ID" in error_str

    assert error.suggestion is not None


def test_dataflow_error_inheritance():
    """Test that DataflowError is a BytewaxRuntimeError."""
    error = DataflowError("Test error")

    assert isinstance(error, DataflowError)
    assert isinstance(error, BytewaxRuntimeError)
    assert isinstance(error, RuntimeError)


def test_dataflow_error_can_be_raised():
    """Test that DataflowError can be raised and caught."""
    with raises(DataflowError) as exc_info:
        raise DataflowError("Test error", suggestion="Fix it")

    assert "Test error" in str(exc_info.value)
    assert exc_info.value.suggestion == "Fix it"


def test_configuration_error_basic():
    """Test ConfigurationError creation."""
    error = ConfigurationError("Invalid recovery configuration")

    error_str = str(error)
    assert "Invalid recovery configuration" in error_str


def test_configuration_error_inheritance():
    """Test that ConfigurationError is a BytewaxRuntimeError."""
    error = ConfigurationError("Test error")

    assert isinstance(error, ConfigurationError)
    assert isinstance(error, BytewaxRuntimeError)
    assert isinstance(error, RuntimeError)


def test_configuration_error_can_be_raised():
    """Test that ConfigurationError can be raised and caught."""
    with raises(ConfigurationError):
        raise ConfigurationError("Test configuration error")


def test_all_error_types_can_be_caught_as_runtime_error():
    """Test that all error types can be caught as RuntimeError."""
    errors = [
        BytewaxRuntimeError("test"),
        OperatorError("step", "op", "msg"),
        DataflowError("msg"),
        ConfigurationError("msg"),
    ]

    for error in errors:
        try:
            raise error
        except RuntimeError:
            # Should be catchable as RuntimeError
            pass


def test_operator_error_chaining():
    """Test that OperatorError can chain from other exceptions."""
    try:
        try:
            raise ValueError("Original error")
        except ValueError as e:
            raise OperatorError(
                step_id="test.op", operator_name="map", message="Processing failed"
            ) from e
    except OperatorError as op_err:
        assert op_err.__cause__ is not None
        assert isinstance(op_err.__cause__, ValueError)
        assert str(op_err.__cause__) == "Original error"


def test_dataflow_error_chaining():
    """Test that DataflowError can chain from other exceptions."""
    try:
        try:
            raise TypeError("Type error")
        except TypeError as e:
            raise DataflowError("Invalid dataflow configuration") from e
    except DataflowError as df_err:
        assert df_err.__cause__ is not None
        assert isinstance(df_err.__cause__, TypeError)
