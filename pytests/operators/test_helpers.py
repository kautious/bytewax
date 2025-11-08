"""Tests for helper operators."""

import pytest

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators.helpers import (
    deduplicate,
    default_value,
    map_dict_value,
    sample,
    take,
    tee,
)
from bytewax.testing import TestingSink, TestingSource, run_main


def test_map_dict_value_existing():
    """Test the existing map_dict_value function."""
    flow = Dataflow("test_map_dict_value")
    s = op.input(
        "inp",
        flow,
        TestingSource(
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
            ]
        ),
    )

    # Map the name field to uppercase
    s = op.map("normalize", s, map_dict_value("name", str.upper))

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    assert len(out) == 2
    assert out[0]["name"] == "ALICE"
    assert out[0]["age"] == 30
    assert out[1]["name"] == "BOB"
    assert out[1]["age"] == 25


def test_deduplicate_simple():
    """Test deduplication with simple values."""
    flow = Dataflow("test_deduplicate_simple")
    s = op.input("inp", flow, TestingSource([1, 2, 2, 3, 1, 4, 3, 5]))

    # Deduplicate
    s = deduplicate("dedup", s)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Should only have unique values
    assert len(out) == 5
    assert set(out) == {1, 2, 3, 4, 5}


def test_deduplicate_with_key_fn():
    """Test deduplication with custom key function."""
    flow = Dataflow("test_deduplicate_key")
    s = op.input(
        "inp",
        flow,
        TestingSource(
            [
                {"id": "1", "name": "Alice"},
                {"id": "2", "name": "Bob"},
                {"id": "1", "name": "Alice Updated"},  # Duplicate ID
                {"id": "3", "name": "Charlie"},
            ]
        ),
    )

    # Deduplicate by ID
    s = deduplicate("dedup", s, key_fn=lambda x: x["id"])

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Should only have 3 items (first occurrence of each ID)
    assert len(out) == 3
    ids = [item["id"] for item in out]
    assert ids == ["1", "2", "3"]
    # Should keep first occurrence
    names = [item["name"] for item in out]
    assert "Alice" in names
    assert "Alice Updated" not in names


def test_deduplicate_empty_stream():
    """Test deduplication with empty stream."""
    flow = Dataflow("test_deduplicate_empty")
    s = op.input("inp", flow, TestingSource([]))

    s = deduplicate("dedup", s)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    assert len(out) == 0


def test_deduplicate_all_unique():
    """Test deduplication when all items are unique."""
    flow = Dataflow("test_deduplicate_unique")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))

    s = deduplicate("dedup", s)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    assert len(out) == 5
    assert out == [1, 2, 3, 4, 5]


def test_deduplicate_all_duplicates():
    """Test deduplication when all items are duplicates."""
    flow = Dataflow("test_deduplicate_all_dup")
    s = op.input("inp", flow, TestingSource([1, 1, 1, 1, 1]))

    s = deduplicate("dedup", s)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Should only keep first occurrence
    assert len(out) == 1
    assert out[0] == 1


def test_sample_basic():
    """Test basic sampling functionality."""
    flow = Dataflow("test_sample_basic")
    items = list(range(1000))
    s = op.input("inp", flow, TestingSource(items))

    # Sample 10% with fixed seed
    s = sample("sample", s, rate=0.1, seed=42)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Should sample approximately 10% (allow some variance)
    assert 50 < len(out) < 150  # Roughly 100 +/- 50
    # All sampled items should be from original
    assert all(item in items for item in out)


def test_sample_deterministic():
    """Test that sampling with same seed is deterministic."""
    # Run 1
    flow1 = Dataflow("test_sample_det1")
    s1 = op.input("inp", flow1, TestingSource(list(range(100))))
    s1 = sample("sample", s1, rate=0.2, seed=12345)
    out1 = []
    op.output("out", s1, TestingSink(out1))
    run_main(flow1)

    # Run 2 with same seed
    flow2 = Dataflow("test_sample_det2")
    s2 = op.input("inp", flow2, TestingSource(list(range(100))))
    s2 = sample("sample", s2, rate=0.2, seed=12345)
    out2 = []
    op.output("out", s2, TestingSink(out2))
    run_main(flow2)

    # Should produce same results
    assert out1 == out2


def test_sample_rate_zero():
    """Test sampling with rate 0 (should filter all)."""
    flow = Dataflow("test_sample_zero")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))

    s = sample("sample", s, rate=0.0, seed=42)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Should filter everything
    assert len(out) == 0


def test_sample_rate_one():
    """Test sampling with rate 1.0 (should keep all)."""
    flow = Dataflow("test_sample_one")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))

    s = sample("sample", s, rate=1.0, seed=42)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Should keep everything
    assert len(out) == 5
    assert out == [1, 2, 3, 4, 5]


def test_take_basic():
    """Test basic take functionality."""
    flow = Dataflow("test_take_basic")
    s = op.input("inp", flow, TestingSource(list(range(100))))

    s = take("take", s, n=10)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Should take first 10 items
    assert len(out) == 10
    assert out == list(range(10))


def test_take_more_than_available():
    """Test taking more items than available."""
    flow = Dataflow("test_take_more")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))

    s = take("take", s, n=10)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Should return all available items
    assert len(out) == 3
    assert out == [1, 2, 3]


def test_take_zero():
    """Test taking zero items."""
    flow = Dataflow("test_take_zero")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))

    s = take("take", s, n=0)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Should return no items
    assert len(out) == 0


def test_take_one():
    """Test taking one item."""
    flow = Dataflow("test_take_one")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))

    s = take("take", s, n=1)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Should return first item only
    assert len(out) == 1
    assert out == [1]


def test_take_empty_stream():
    """Test take on empty stream."""
    flow = Dataflow("test_take_empty")
    s = op.input("inp", flow, TestingSource([]))

    s = take("take", s, n=10)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    assert len(out) == 0


def test_tee_basic():
    """Test basic tee functionality."""
    flow = Dataflow("test_tee_basic")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))

    # Tee the stream
    s1, s2 = tee("tee", s)

    # Process each branch differently
    evens = op.filter("evens", s1, lambda x: x % 2 == 0)
    odds = op.filter("odds", s2, lambda x: x % 2 == 1)

    out_evens = []
    out_odds = []
    op.output("out_evens", evens, TestingSink(out_evens))
    op.output("out_odds", odds, TestingSink(out_odds))

    run_main(flow)

    assert out_evens == [2, 4]
    assert out_odds == [1, 3, 5]


def test_tee_same_processing():
    """Test tee with same processing on both branches."""
    flow = Dataflow("test_tee_same")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))

    s1, s2 = tee("tee", s)

    # Apply same transformation to both
    s1 = op.map("double1", s1, lambda x: x * 2)
    s2 = op.map("double2", s2, lambda x: x * 2)

    out1 = []
    out2 = []
    op.output("out1", s1, TestingSink(out1))
    op.output("out2", s2, TestingSink(out2))

    run_main(flow)

    # Both should have same results
    assert out1 == [2, 4, 6]
    assert out2 == [2, 4, 6]


def test_tee_one_branch_unused():
    """Test tee where one branch is not used."""
    flow = Dataflow("test_tee_unused")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))

    s1, s2 = tee("tee", s)

    # Only use one branch
    out = []
    op.output("out", s1, TestingSink(out))

    run_main(flow)

    assert out == [1, 2, 3]


def test_default_value_with_nones():
    """Test replacing None values with default."""
    flow = Dataflow("test_default_value")
    s = op.input("inp", flow, TestingSource([1, None, 2, None, 3]))

    s = default_value("fill", s, default=0)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # None values should be replaced with 0
    assert out == [1, 0, 2, 0, 3]


def test_default_value_no_nones():
    """Test default_value when there are no None values."""
    flow = Dataflow("test_default_no_none")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))

    s = default_value("fill", s, default=0)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Should be unchanged
    assert out == [1, 2, 3, 4, 5]


def test_default_value_all_nones():
    """Test default_value when all values are None."""
    flow = Dataflow("test_default_all_none")
    s = op.input("inp", flow, TestingSource([None, None, None]))

    s = default_value("fill", s, default=-1)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # All should be replaced
    assert out == [-1, -1, -1]


def test_default_value_with_string():
    """Test default_value with string default."""
    flow = Dataflow("test_default_string")
    s = op.input("inp", flow, TestingSource(["hello", None, "world", None]))

    s = default_value("fill", s, default="<empty>")

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    assert out == ["hello", "<empty>", "world", "<empty>"]


def test_default_value_empty_stream():
    """Test default_value on empty stream."""
    flow = Dataflow("test_default_empty")
    s = op.input("inp", flow, TestingSource([]))

    s = default_value("fill", s, default=0)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    assert len(out) == 0


def test_helpers_composition():
    """Test composing multiple helper operators."""
    flow = Dataflow("test_helpers_compose")
    s = op.input(
        "inp", flow, TestingSource([1, 2, 2, 3, None, 4, 4, 5, None, 6, 7, 8, 9, 10])
    )

    # Compose helpers: deduplicate -> fill None -> sample -> take
    s = deduplicate("dedup", s)
    s = default_value("fill", s, default=0)
    s = sample("sample", s, rate=1.0, seed=42)  # Keep all for testing
    s = take("take", s, n=5)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Should have deduplicated, filled None, and taken first 5
    assert len(out) == 5
    # Should not have duplicates
    assert len(set(out)) == len(out)
    # Should not have None
    assert None not in out


def test_helpers_with_keyed_stream():
    """Test helper operators work with map operations."""
    flow = Dataflow("test_helpers_keyed")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))

    # Map, then deduplicate
    s = op.map("mod", s, lambda x: x % 3)  # Maps to 1,2,0,1,2
    s = deduplicate("dedup", s)

    out = []
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Should have only unique mod values
    assert len(out) == 3
    assert set(out) == {0, 1, 2}
