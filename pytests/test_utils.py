"""Tests for utility functions in _utils.py."""

from bytewax._utils import partition


def test_partition_basic():
    """Test basic partitioning."""
    items = [1, 2, 3, 4, 5, 6]
    trues, falses = partition(items, lambda x: x % 2 == 0)

    assert trues == [2, 4, 6]
    assert falses == [1, 3, 5]


def test_partition_all_true():
    """Test when all items match predicate."""
    items = [2, 4, 6, 8]
    trues, falses = partition(items, lambda x: x % 2 == 0)

    assert trues == [2, 4, 6, 8]
    assert falses == []


def test_partition_all_false():
    """Test when no items match predicate."""
    items = [1, 3, 5, 7]
    trues, falses = partition(items, lambda x: x % 2 == 0)

    assert trues == []
    assert falses == [1, 3, 5, 7]


def test_partition_empty():
    """Test with empty iterable."""
    items = []
    trues, falses = partition(items, lambda x: x % 2 == 0)

    assert trues == []
    assert falses == []


def test_partition_strings():
    """Test partitioning strings."""
    items = ["apple", "banana", "apricot", "blueberry", "avocado"]
    trues, falses = partition(items, lambda x: x.startswith("a"))

    assert trues == ["apple", "apricot", "avocado"]
    assert falses == ["banana", "blueberry"]


def test_partition_mixed_types():
    """Test partitioning with type checking."""
    items = [1, "hello", 2, "world", 3]
    trues, falses = partition(items, lambda x: isinstance(x, int))

    assert trues == [1, 2, 3]
    assert falses == ["hello", "world"]


def test_partition_complex_predicate():
    """Test with complex predicate."""
    items = [
        {"name": "Alice", "age": 25},
        {"name": "Bob", "age": 30},
        {"name": "Charlie", "age": 20},
        {"name": "Diana", "age": 35},
    ]
    trues, falses = partition(items, lambda x: x["age"] >= 30)

    assert trues == [{"name": "Bob", "age": 30}, {"name": "Diana", "age": 35}]
    assert falses == [{"name": "Alice", "age": 25}, {"name": "Charlie", "age": 20}]


def test_partition_generator():
    """Test that partition works with generators."""
    gen = (x for x in range(10))
    trues, falses = partition(gen, lambda x: x < 5)

    assert trues == [0, 1, 2, 3, 4]
    assert falses == [5, 6, 7, 8, 9]


def test_partition_preserves_order():
    """Test that partition preserves original order."""
    items = [5, 2, 8, 1, 9, 3, 7, 4, 6]
    trues, falses = partition(items, lambda x: x % 2 == 0)

    # Even numbers in order they appeared
    assert trues == [2, 8, 4, 6]
    # Odd numbers in order they appeared
    assert falses == [5, 1, 9, 3, 7]
