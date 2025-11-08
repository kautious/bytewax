"""Tests for dataflow location and loading in run.py."""

import re
import tempfile
from pathlib import Path

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.run import _locate_dataflow
from bytewax.testing import TestingSource
from pytest import raises


def test_locate_dataflow_simple_variable(tmp_path):
    """Test locating a simple dataflow variable."""
    # Create a temporary module with a dataflow
    module_path = tmp_path / "test_module.py"
    module_path.write_text(
        """
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource

flow = Dataflow("test")
inp = op.input("inp", flow, TestingSource([1, 2, 3]))
"""
    )

    # Add tmp_path to sys.path temporarily
    import sys

    sys.path.insert(0, str(tmp_path))
    try:
        dataflow = _locate_dataflow("test_module", "flow")
        assert isinstance(dataflow, Dataflow)
        assert dataflow.flow_id == "test"
    finally:
        sys.path.remove(str(tmp_path))


def test_locate_dataflow_function_call(tmp_path):
    """Test locating a dataflow created by function."""
    module_path = tmp_path / "test_factory.py"
    module_path.write_text(
        """
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource

def create_flow():
    flow = Dataflow("factory_flow")
    inp = op.input("inp", flow, TestingSource([1, 2, 3]))
    return flow
"""
    )

    import sys

    sys.path.insert(0, str(tmp_path))
    try:
        dataflow = _locate_dataflow("test_factory", "create_flow()")
        assert isinstance(dataflow, Dataflow)
        assert dataflow.flow_id == "factory_flow"
    finally:
        sys.path.remove(str(tmp_path))


def test_locate_dataflow_function_with_args(tmp_path):
    """Test locating a dataflow with function arguments."""
    module_path = tmp_path / "test_args.py"
    module_path.write_text(
        """
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource

def create_flow(name, count):
    flow = Dataflow(name)
    inp = op.input("inp", flow, TestingSource(range(count)))
    return flow
"""
    )

    import sys

    sys.path.insert(0, str(tmp_path))
    try:
        dataflow = _locate_dataflow("test_args", "create_flow('custom', 5)")
        assert isinstance(dataflow, Dataflow)
        assert dataflow.flow_id == "custom"
    finally:
        sys.path.remove(str(tmp_path))


def test_locate_dataflow_missing_module():
    """Test error when module doesn't exist."""
    expect = "No module named 'nonexistent_module'"
    with raises(ImportError, match=re.escape(expect)):
        _locate_dataflow("nonexistent_module", "flow")


def test_locate_dataflow_missing_attribute(tmp_path):
    """Test error when attribute doesn't exist in module."""
    module_path = tmp_path / "test_missing.py"
    module_path.write_text(
        """
from bytewax.dataflow import Dataflow

other_var = "not a dataflow"
"""
    )

    import sys

    sys.path.insert(0, str(tmp_path))
    try:
        expect = "Failed to find attribute 'flow' in 'test_missing'"
        with raises(AttributeError, match=re.escape(expect)):
            _locate_dataflow("test_missing", "flow")
    finally:
        sys.path.remove(str(tmp_path))


def test_locate_dataflow_not_a_dataflow(tmp_path):
    """Test error when attribute is not a Dataflow."""
    module_path = tmp_path / "test_wrong_type.py"
    module_path.write_text(
        """
flow = "not a dataflow object"
"""
    )

    import sys

    sys.path.insert(0, str(tmp_path))
    try:
        expect = "A valid Bytewax dataflow was not obtained"
        with raises(RuntimeError, match=re.escape(expect)):
            _locate_dataflow("test_wrong_type", "flow")
    finally:
        sys.path.remove(str(tmp_path))


def test_locate_dataflow_invalid_syntax(tmp_path):
    """Test error with invalid dataflow name syntax."""
    # Create a module so we get past the import
    module_path = tmp_path / "test_syntax.py"
    module_path.write_text("flow = None")

    import sys
    sys.path.insert(0, str(tmp_path))
    try:
        # This will fail during parsing
        expect = "Failed to parse"
        with raises((SyntaxError, ValueError), match=re.escape(expect)):
            _locate_dataflow("test_syntax", "flow['invalid']")
    finally:
        sys.path.remove(str(tmp_path))


def test_locate_dataflow_function_wrong_args(tmp_path):
    """Test error when function called with wrong arguments."""
    module_path = tmp_path / "test_wrong_args.py"
    module_path.write_text(
        """
from bytewax.dataflow import Dataflow

def create_flow(required_arg):
    return Dataflow(required_arg)
"""
    )

    import sys

    sys.path.insert(0, str(tmp_path))
    try:
        expect = "could not be called with the specified arguments"
        with raises(TypeError, match=re.escape(expect)):
            _locate_dataflow("test_wrong_args", "create_flow()")
    finally:
        sys.path.remove(str(tmp_path))


def test_locate_dataflow_with_kwargs(tmp_path):
    """Test locating dataflow with keyword arguments."""
    module_path = tmp_path / "test_kwargs.py"
    module_path.write_text(
        """
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource

def create_flow(name="default", multiplier=1):
    flow = Dataflow(name)
    inp = op.input("inp", flow, TestingSource([i * multiplier for i in range(3)]))
    return flow
"""
    )

    import sys

    sys.path.insert(0, str(tmp_path))
    try:
        dataflow = _locate_dataflow(
            "test_kwargs", "create_flow(name='kwargs_test', multiplier=2)"
        )
        assert isinstance(dataflow, Dataflow)
        assert dataflow.flow_id == "kwargs_test"
    finally:
        sys.path.remove(str(tmp_path))


def test_locate_dataflow_complex_expression_fails(tmp_path):
    """Test that complex expressions are rejected."""
    module_path = tmp_path / "test_complex.py"
    module_path.write_text(
        """
from bytewax.dataflow import Dataflow

def create_flow():
    return Dataflow("test")
"""
    )

    import sys

    sys.path.insert(0, str(tmp_path))
    try:
        # Complex expressions with method calls should fail
        expect = "Function reference must be a simple name"
        with raises(TypeError, match=re.escape(expect)):
            _locate_dataflow("test_complex", "module.create_flow()")
    finally:
        sys.path.remove(str(tmp_path))
