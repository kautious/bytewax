"""Tests for fluent API extensions."""

import bytewax.operators as op
from bytewax.dataflow import Dataflow, Stream
from bytewax.fluent import FluentStream, add_fluent_methods, chain
from bytewax.testing import TestingSink, TestingSource, run_main


def test_fluent_stream_map():
    """Test FluentStream map method."""
    out = []

    flow = Dataflow("test_fluent_map")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = FluentStream(s).map("double", lambda x: x * 2).unwrap()
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [2, 4, 6]


def test_fluent_stream_filter():
    """Test FluentStream filter method."""
    out = []

    flow = Dataflow("test_fluent_filter")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    s = FluentStream(s).filter("evens", lambda x: x % 2 == 0).unwrap()
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [2, 4]


def test_fluent_stream_chaining():
    """Test chaining multiple fluent methods."""
    out = []

    flow = Dataflow("test_fluent_chain")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    s = (
        FluentStream(s)
        .filter("evens", lambda x: x % 2 == 0)
        .map("multiply", lambda x: x * 10)
        .unwrap()
    )
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [20, 40]


def test_fluent_stream_flat_map():
    """Test FluentStream flat_map method."""
    out = []

    flow = Dataflow("test_fluent_flat_map")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = FluentStream(s).flat_map("expand", lambda x: [x, x * 2]).unwrap()
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [1, 2, 2, 4, 3, 6]


def test_fluent_stream_key_on():
    """Test FluentStream key_on method."""
    out = []

    flow = Dataflow("test_fluent_key_on")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = FluentStream(s).key_on("key", lambda x: str(x % 2)).unwrap()
    op.output("out", s, TestingSink(out))

    run_main(flow)
    # Should be keyed tuples
    assert all(isinstance(item, tuple) for item in out)
    assert all(len(item) == 2 for item in out)


def test_fluent_stream_inspect():
    """Test FluentStream inspect method."""
    out = []
    inspected = []

    flow = Dataflow("test_fluent_inspect")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = FluentStream(s).inspect_debug("debug", inspected.append).unwrap()
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [1, 2, 3]
    assert inspected == [1, 2, 3]


def test_fluent_stream_collect():
    """Test FluentStream collect method."""
    out = []

    flow = Dataflow("test_fluent_collect")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    s = FluentStream(s).collect("batch", max_size=2).unwrap()
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert len(out) == 3  # [1,2], [3,4], [5]
    assert out[0] == [1, 2]
    assert out[1] == [3, 4]
    assert out[2] == [5]


def test_fluent_stream_output():
    """Test FluentStream output method."""
    out = []

    flow = Dataflow("test_fluent_output")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    FluentStream(s).output("out", TestingSink(out))

    run_main(flow)
    assert out == [1, 2, 3]


def test_chain_helper():
    """Test chain() convenience function."""
    out = []

    flow = Dataflow("test_chain")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    s = chain(s).filter("evens", lambda x: x % 2 == 0).map("double", lambda x: x * 2).unwrap()
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [4, 8]


def test_add_fluent_methods_stream():
    """Test add_fluent_methods() for Stream class."""
    add_fluent_methods()

    # Now Stream should have fluent methods
    assert hasattr(Stream, "map")
    assert hasattr(Stream, "filter")
    assert hasattr(Stream, "flat_map")


def test_add_fluent_methods_dataflow():
    """Test add_fluent_methods() for Dataflow class."""
    add_fluent_methods()

    # Now Dataflow should have input method
    assert hasattr(Dataflow, "input")


def test_fluent_methods_usage():
    """Test using fluent methods after add_fluent_methods()."""
    add_fluent_methods()

    out = []

    flow = Dataflow("test_fluent_usage")
    # Use fluent input method
    s = flow.input("inp", TestingSource([1, 2, 3, 4, 5]))  # type: ignore
    # Use fluent stream methods
    s = s.filter("evens", lambda x: x % 2 == 0)  # type: ignore
    s = s.map("triple", lambda x: x * 3)  # type: ignore
    s.output("out", TestingSink(out))  # type: ignore

    run_main(flow)
    assert out == [6, 12]


def test_fluent_complex_pipeline():
    """Test complex pipeline with fluent API."""
    out = []

    flow = Dataflow("test_complex_fluent")
    s = op.input("inp", flow, TestingSource(range(10)))
    s = (
        FluentStream(s)
        .filter("gt_2", lambda x: x > 2)
        .filter("lt_8", lambda x: x < 8)
        .map("square", lambda x: x * x)
        .filter("not_25", lambda x: x != 25)
        .unwrap()
    )
    op.output("out", s, TestingSink(out))

    run_main(flow)
    # 3,4,5,6,7 -> squared -> 9,16,25,36,49 -> filter 25 -> 9,16,36,49
    assert out == [9, 16, 36, 49]


def test_fluent_stream_unwrap():
    """Test unwrap returns original stream type."""
    flow = Dataflow("test_unwrap")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))

    fluent = FluentStream(s)
    unwrapped = fluent.unwrap()

    assert isinstance(unwrapped, Stream)
    assert unwrapped == s


def test_fluent_preserves_stream_behavior():
    """Test that fluent operations preserve stream behavior."""
    out_fluent = []
    out_normal = []

    # Using fluent API
    flow1 = Dataflow("fluent")
    s1 = op.input("inp", flow1, TestingSource([1, 2, 3]))
    s1 = FluentStream(s1).map("double", lambda x: x * 2).unwrap()
    op.output("out", s1, TestingSink(out_fluent))
    run_main(flow1)

    # Using normal API
    flow2 = Dataflow("normal")
    s2 = op.input("inp", flow2, TestingSource([1, 2, 3]))
    s2 = op.map("double", s2, lambda x: x * 2)
    op.output("out", s2, TestingSink(out_normal))
    run_main(flow2)

    # Should have identical results
    assert out_fluent == out_normal


def test_fluent_with_keyed_stream():
    """Test fluent API with keyed stream operations."""
    out = []

    flow = Dataflow("test_keyed_fluent")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4]))
    s = (
        FluentStream(s)
        .key_on("key", lambda x: str(x % 2))
        .unwrap()
    )
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Should have keyed tuples
    keys = [key for key, _ in out]
    assert "0" in keys  # Even numbers
    assert "1" in keys  # Odd numbers


def test_add_fluent_methods_idempotent():
    """Test that add_fluent_methods can be called multiple times."""
    # Call multiple times
    add_fluent_methods()
    add_fluent_methods()
    add_fluent_methods()

    # Should still work
    assert hasattr(Stream, "map")
    assert hasattr(Dataflow, "input")
