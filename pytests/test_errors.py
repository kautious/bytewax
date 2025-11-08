"""Tests for error handling in errors.py."""

import re

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.errors import BytewaxRuntimeError
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

    expect = "Error at item 2"
    with raises(BytewaxRuntimeError, match=re.escape(expect)):
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
