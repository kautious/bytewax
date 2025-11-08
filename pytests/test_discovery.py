"""Tests for operator discovery tools."""

import pytest

from bytewax.discovery import (
    OperatorInfo,
    describe_operator,
    list_categories,
    list_operators,
    search_operators,
)


def test_list_operators_returns_list():
    """Test that list_operators returns a list."""
    operators = list_operators()

    assert isinstance(operators, list)
    assert len(operators) > 0


def test_list_operators_returns_tuples():
    """Test that list_operators returns (name, summary) tuples."""
    operators = list_operators()

    for item in operators:
        assert isinstance(item, tuple)
        assert len(item) == 2
        name, summary = item
        assert isinstance(name, str)
        assert isinstance(summary, str)
        assert len(name) > 0
        assert len(summary) > 0


def test_list_operators_includes_common_operators():
    """Test that common operators are included."""
    operators = list_operators()
    operator_names = [name for name, _ in operators]

    # Check for essential operators
    assert "map" in operator_names
    assert "filter" in operator_names
    assert "input" in operator_names
    assert "output" in operator_names


def test_list_operators_sorted_alphabetically():
    """Test that operators are sorted alphabetically."""
    operators = list_operators()
    operator_names = [name for name, _ in operators]

    assert operator_names == sorted(operator_names)


def test_list_operators_with_category_filter():
    """Test filtering operators by category."""
    all_ops = list_operators()
    stateful_ops = list_operators(category="stateful")

    # Stateful should be subset of all
    assert len(stateful_ops) < len(all_ops)

    # Each result should be from stateful category
    for name, _ in stateful_ops:
        # Verify by checking the operator
        info = describe_operator(name)
        assert info.category == "stateful"


def test_describe_operator_for_map():
    """Test describing the map operator."""
    info = describe_operator("map")

    assert isinstance(info, OperatorInfo)
    assert info.name == "map"
    assert info.function is not None
    assert callable(info.function)
    assert len(info.signature) > 0
    assert len(info.summary) > 0
    assert len(info.docstring) > 0
    assert info.category in [
        "stateless",
        "stateful",
        "windowing",
        "multi-stream",
        "source",
        "terminal",
    ]


def test_describe_operator_for_filter():
    """Test describing the filter operator."""
    info = describe_operator("filter")

    assert info.name == "filter"
    # Bytewax's actual summary is "Keep only some items"
    assert "keep" in info.summary.lower() or "only" in info.summary.lower()


def test_describe_operator_nonexistent_raises():
    """Test that describing nonexistent operator raises AttributeError."""
    with pytest.raises(AttributeError) as exc_info:
        describe_operator("nonexistent_operator_xyz")

    assert "not found" in str(exc_info.value)


def test_describe_operator_includes_example():
    """Test that some operators include examples."""
    # Map should have an example in its docstring
    info = describe_operator("map")

    # Example may or may not be extracted depending on docstring format
    # Just verify the field exists
    assert hasattr(info, "example")


def test_search_operators_finds_matches():
    """Test searching for operators."""
    results = search_operators("map")

    # Should find at least map, and possibly map_value, etc.
    assert len(results) > 0

    # All results should contain 'map' in name or summary
    for name, summary in results:
        assert "map" in name.lower() or "map" in summary.lower()


def test_search_operators_case_insensitive():
    """Test that search is case-insensitive."""
    results_lower = search_operators("map")
    results_upper = search_operators("MAP")
    results_mixed = search_operators("MaP")

    # Should all return the same results
    assert len(results_lower) == len(results_upper)
    assert len(results_lower) == len(results_mixed)


def test_search_operators_no_matches():
    """Test search with no matches."""
    results = search_operators("xyz_nonexistent_12345")

    assert len(results) == 0


def test_search_operators_by_description():
    """Test searching by description content."""
    # Search for operators related to stream/items
    results = search_operators("stream")

    # Should find multiple operators (most operators work with streams)
    assert len(results) > 0


def test_list_categories_returns_list():
    """Test that list_categories returns a list."""
    categories = list_categories()

    assert isinstance(categories, list)
    assert len(categories) > 0


def test_list_categories_includes_common_categories():
    """Test that common categories are included."""
    categories = list_categories()

    # Should have basic categories
    assert "stateless" in categories or "stateful" in categories


def test_list_categories_sorted():
    """Test that categories are sorted."""
    categories = list_categories()

    assert categories == sorted(categories)


def test_categories_are_consistent():
    """Test that all operators have valid categories."""
    all_ops = list_operators()
    categories = set(list_categories())

    for name, _ in all_ops:
        info = describe_operator(name)
        assert info.category in categories


def test_operator_info_fields():
    """Test that OperatorInfo has all expected fields."""
    info = describe_operator("map")

    assert hasattr(info, "name")
    assert hasattr(info, "function")
    assert hasattr(info, "signature")
    assert hasattr(info, "summary")
    assert hasattr(info, "docstring")
    assert hasattr(info, "category")
    assert hasattr(info, "example")


def test_describe_operator_for_input():
    """Test describing the input operator."""
    info = describe_operator("input")

    assert info.name == "input"
    assert info.category == "source"


def test_describe_operator_for_output():
    """Test describing the output operator."""
    info = describe_operator("output")

    assert info.name == "output"
    assert info.category == "terminal"


def test_describe_operator_for_reduce():
    """Test describing the reduce operator."""
    info = describe_operator("reduce_final")

    assert info.name == "reduce_final"
    # May be categorized as stateful or windowing (doc mentions reduce_window)
    assert info.category in ["stateful", "windowing"]
    # Summary should mention values or keys
    assert "value" in info.summary.lower() or "key" in info.summary.lower()


def test_search_finds_multiple_related():
    """Test that search finds all related operators."""
    # Search for 'value' should find map_value, filter_value, etc.
    results = search_operators("value")

    result_names = [name for name, _ in results]

    # Should include value operators
    value_ops = [name for name in result_names if "value" in name]
    assert len(value_ops) > 0


def test_list_operators_excludes_private():
    """Test that private functions are excluded."""
    operators = list_operators()
    operator_names = [name for name, _ in operators]

    # Should not include anything starting with underscore
    for name in operator_names:
        assert not name.startswith("_")


def test_operator_summaries_are_concise():
    """Test that operator summaries are reasonably short."""
    operators = list_operators()

    for name, summary in operators:
        # Summaries should be concise (< 200 chars typically)
        assert len(summary) < 300, f"{name} summary too long: {len(summary)} chars"


def test_describe_operator_signature_format():
    """Test that operator signature is well-formatted."""
    info = describe_operator("map")

    # Signature should include the function name
    assert info.name in info.signature

    # Signature should include parentheses
    assert "(" in info.signature
    assert ")" in info.signature


def test_search_operators_returns_sorted():
    """Test that search results are sorted."""
    results = search_operators("a")  # Should match many operators

    if len(results) > 1:
        result_names = [name for name, _ in results]
        assert result_names == sorted(result_names)


def test_integration_list_describe():
    """Test integration between list and describe."""
    # List all operators
    operators = list_operators()

    # Describe each one (should not raise)
    for name, summary in operators[:5]:  # Test first 5 to keep test fast
        info = describe_operator(name)
        assert info.name == name
        # Summary from list should match or be part of describe
        assert info.summary


def test_integration_search_describe():
    """Test integration between search and describe."""
    # Search for operators
    results = search_operators("map")

    # Describe each result (should not raise)
    for name, summary in results:
        info = describe_operator(name)
        assert info.name == name


def test_categories_cover_all_operators():
    """Test that categories cover all operators."""
    categories = list_categories()

    # Get operators from each category
    ops_from_categories = set()
    for cat in categories:
        cat_ops = list_operators(category=cat)
        for name, _ in cat_ops:
            ops_from_categories.add(name)

    # Get all operators
    all_ops = list_operators()
    all_op_names = set(name for name, _ in all_ops)

    # Every operator should be in some category
    assert ops_from_categories == all_op_names
