"""Edge case tests for various Bytewax functionality."""

import re

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import raises


def test_empty_input_stream():
    """Test that empty input streams are handled correctly."""
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([]))
    s = op.map("double", s, lambda x: x * 2)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == []


def test_single_item_stream():
    """Test stream with single item."""
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([42]))
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [42]


def test_large_batch_processing():
    """Test processing large batches of data."""
    out = []
    large_input = list(range(10000))

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(large_input))
    s = op.map("add_one", s, lambda x: x + 1)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert len(out) == 10000
    assert out[0] == 1
    assert out[-1] == 10000


def test_none_values_in_stream():
    """Test handling None values in stream."""
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, None, 3, None, 5]))
    s = op.map("identity", s, lambda x: x)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [1, None, 3, None, 5]


def test_filter_all_items():
    """Test filter that removes all items."""
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    s = op.filter("remove_all", s, lambda x: False)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == []


def test_filter_no_items():
    """Test filter that keeps all items."""
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    s = op.filter("keep_all", s, lambda x: True)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [1, 2, 3, 4, 5]


def test_flat_map_empty_expansion():
    """Test flat_map that produces no items."""
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = op.flat_map("empty", s, lambda x: [])
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == []


def test_flat_map_large_expansion():
    """Test flat_map that expands items significantly."""
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2]))
    s = op.flat_map("expand", s, lambda x: [x] * 100)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert len(out) == 200
    assert out[:100] == [1] * 100
    assert out[100:] == [2] * 100


def test_map_with_exception():
    """Test that exceptions in map propagate correctly."""
    out = []

    def raise_error(x):
        if x == 3:
            raise ValueError("Error at 3")
        return x * 2

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4]))
    s = op.map("error_map", s, raise_error)
    op.output("out", s, TestingSink(out))

    expect = "Error at 3"
    with raises(ValueError, match=re.escape(expect)):
        run_main(flow)


def test_branch_with_all_true():
    """Test branch where all items go to trues."""
    trues_out = []
    falses_out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([2, 4, 6, 8]))
    branches = op.branch("split", s, lambda x: True)
    op.output("trues", branches.trues, TestingSink(trues_out))
    op.output("falses", branches.falses, TestingSink(falses_out))

    run_main(flow)
    assert trues_out == [2, 4, 6, 8]
    assert falses_out == []


def test_branch_with_all_false():
    """Test branch where all items go to falses."""
    trues_out = []
    falses_out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 3, 5, 7]))
    branches = op.branch("split", s, lambda x: False)
    op.output("trues", branches.trues, TestingSink(trues_out))
    op.output("falses", branches.falses, TestingSink(falses_out))

    run_main(flow)
    assert trues_out == []
    assert falses_out == [1, 3, 5, 7]


def test_merge_empty_streams():
    """Test merging when all streams are empty."""
    out = []

    flow = Dataflow("test_df")
    s1 = op.input("inp1", flow, TestingSource([]))
    s2 = op.input("inp2", flow, TestingSource([]))
    merged = op.merge("merge", s1, s2)
    op.output("out", merged, TestingSink(out))

    run_main(flow)
    assert out == []


def test_merge_multiple_streams():
    """Test merging more than two streams."""
    out = []

    flow = Dataflow("test_df")
    s1 = op.input("inp1", flow, TestingSource([1]))
    s2 = op.input("inp2", flow, TestingSource([2]))
    s3 = op.input("inp3", flow, TestingSource([3]))
    merged = op.merge("merge", s1, s2, s3)
    op.output("out", merged, TestingSink(out))

    run_main(flow)
    # Order is not guaranteed in merge
    assert sorted(out) == [1, 2, 3]


def test_key_on_with_duplicate_keys():
    """Test key_on with duplicate keys."""
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    # All items get same key
    keyed = op.key_on("key", s, lambda x: "same_key")
    op.output("out", keyed, TestingSink(out))

    run_main(flow)
    assert out == [
        ("same_key", 1),
        ("same_key", 2),
        ("same_key", 3),
        ("same_key", 4),
        ("same_key", 5),
    ]


def test_map_value_on_keyed_stream():
    """Test map_value preserves keys."""
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([("a", 1), ("b", 2), ("c", 3)]))
    s = op.map_value("double", s, lambda x: x * 2)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [("a", 2), ("b", 4), ("c", 6)]


def test_filter_value_removes_all():
    """Test filter_value that removes all items."""
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([("a", 1), ("b", 2), ("c", 3)]))
    s = op.filter_value("filter", s, lambda x: False)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == []


def test_inspect_with_no_side_effects():
    """Test that inspect doesn't modify stream."""
    out = []
    inspected = []

    def inspector(x):
        inspected.append(x)

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = op.inspect("inspect", s, inspector)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [1, 2, 3]
    assert inspected == [1, 2, 3]


def test_collect_empty_stream():
    """Test collect on empty stream."""
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([]))
    s = op.collect("collect", s, max_size=10)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == []


def test_collect_single_item():
    """Test collect with single item."""
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([42]))
    s = op.collect("collect", s, max_size=10)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [[42]]


def test_flatten_nested_empty_lists():
    """Test flatten with empty nested lists."""
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([[], [], []]))
    s = op.flatten("flatten", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == []


def test_flatten_mixed_empty_and_full():
    """Test flatten with mix of empty and non-empty lists."""
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([[], [1, 2], [], [3], []]))
    s = op.flatten("flatten", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [1, 2, 3]


def test_multiple_outputs():
    """Test dataflow with multiple output operators."""
    out1 = []
    out2 = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    doubled = op.map("double", s, lambda x: x * 2)
    tripled = op.map("triple", s, lambda x: x * 3)
    op.output("out1", doubled, TestingSink(out1))
    op.output("out2", tripled, TestingSink(out2))

    run_main(flow)
    assert out1 == [2, 4, 6]
    assert out2 == [3, 6, 9]


def test_unicode_in_stream():
    """Test handling Unicode strings in stream."""
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(["hello", "世界", "🎉", "مرحبا"]))
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == ["hello", "世界", "🎉", "مرحبا"]


def test_very_long_strings():
    """Test handling very long strings."""
    out = []
    long_string = "x" * 100000

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([long_string]))
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [long_string]
    assert len(out[0]) == 100000


def test_mixed_types_in_stream():
    """Test stream with mixed Python types."""
    out = []
    mixed_data = [
        42,
        "string",
        3.14,
        [1, 2, 3],
        {"key": "value"},
        (1, 2),
        None,
        True,
        False,
    ]

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(mixed_data))
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == mixed_data
