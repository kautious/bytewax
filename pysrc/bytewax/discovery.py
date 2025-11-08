"""Operator discovery and introspection tools.

This module helps users discover and learn about available operators
in Bytewax.

Example usage:

```python
from bytewax.discovery import list_operators, describe_operator

# List all operators
ops = list_operators()
for name, summary in ops:
    print(f"{name}: {summary}")

# Get detailed information
info = describe_operator('map')
print(info.signature)
print(info.docstring)
print(info.example)

# Search for operators
results = search_operators('window')
for name, summary in results:
    print(f"{name}: {summary}")
```

CLI usage:

```bash
python -m bytewax.discovery list
python -m bytewax.discovery describe map
python -m bytewax.discovery search window
```

"""

import inspect
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

import bytewax.operators as op


@dataclass
class OperatorInfo:
    """Information about an operator."""

    name: str
    """Operator name (e.g., 'map', 'filter')."""

    function: Callable
    """The operator function itself."""

    signature: str
    """Function signature as a string."""

    summary: str
    """One-line summary of what the operator does."""

    docstring: str
    """Full docstring."""

    category: str
    """Category (e.g., 'stateless', 'stateful', 'windowing')."""

    example: Optional[str] = None
    """Example code snippet (if available in docstring)."""


def _extract_summary(docstring: Optional[str]) -> str:
    """Extract first line/sentence from docstring."""
    if not docstring:
        return "No description available"

    # Get first line
    lines = docstring.strip().split("\n")
    first_line = lines[0].strip()

    # Remove trailing period
    if first_line.endswith("."):
        first_line = first_line[:-1]

    return first_line


def _categorize_operator(name: str, docstring: Optional[str]) -> str:
    """Categorize an operator based on name and docstring."""
    stateful_keywords = ["state", "stateful", "reduce", "fold", "accumulate"]
    window_keywords = ["window", "tumbling", "sliding", "session"]
    multi_stream_keywords = ["merge", "join", "concat"]
    terminal_keywords = ["output", "inspect"]

    name_lower = name.lower()
    doc_lower = (docstring or "").lower()

    if any(kw in name_lower or kw in doc_lower for kw in terminal_keywords):
        return "terminal"
    elif any(kw in name_lower or kw in doc_lower for kw in window_keywords):
        return "windowing"
    elif any(kw in name_lower or kw in doc_lower for kw in stateful_keywords):
        return "stateful"
    elif any(kw in name_lower or kw in doc_lower for kw in multi_stream_keywords):
        return "multi-stream"
    elif name_lower in ["input"]:
        return "source"
    else:
        return "stateless"


def _extract_example(docstring: Optional[str]) -> Optional[str]:
    """Extract example code from docstring."""
    if not docstring:
        return None

    # Look for code blocks in docstring (markdown style)
    lines = docstring.split("\n")
    in_code_block = False
    example_lines = []

    for line in lines:
        if "```python" in line or "```{testcode}" in line:
            in_code_block = True
            continue
        elif "```" in line and in_code_block:
            break
        elif in_code_block:
            example_lines.append(line)

    if example_lines:
        return "\n".join(example_lines).strip()

    return None


def list_operators(category: Optional[str] = None) -> List[Tuple[str, str]]:
    """List all available operators.

    Args:
        category: Optional category filter (e.g., 'stateful', 'windowing').

    Returns:
        List of (name, summary) tuples.

    Example:
        ```python
        # List all operators
        all_ops = list_operators()

        # List only stateful operators
        stateful_ops = list_operators(category='stateful')
        ```

    """
    operators = []

    # Get all public functions from operators module
    for name in dir(op):
        if name.startswith("_"):
            continue

        attr = getattr(op, name)

        # Check if it's a callable (function or operator)
        if callable(attr):
            # Get docstring
            docstring = inspect.getdoc(attr)

            # Categorize
            op_category = _categorize_operator(name, docstring)

            # Filter by category if specified
            if category and op_category != category:
                continue

            # Extract summary
            summary = _extract_summary(docstring)

            operators.append((name, summary))

    # Sort alphabetically
    operators.sort(key=lambda x: x[0])

    return operators


def describe_operator(name: str) -> OperatorInfo:
    """Get detailed information about an operator.

    Args:
        name: Operator name (e.g., 'map', 'filter').

    Returns:
        Detailed operator information.

    Raises:
        AttributeError: If operator not found.

    Example:
        ```python
        info = describe_operator('map')
        print(f"Signature: {info.signature}")
        print(f"Category: {info.category}")
        print(f"Summary: {info.summary}")
        ```

    """
    # Get the operator function
    try:
        func = getattr(op, name)
    except AttributeError:
        raise AttributeError(
            f"Operator '{name}' not found. "
            f"Use list_operators() to see available operators."
        )

    # Get signature
    try:
        sig = inspect.signature(func)
        signature = f"{name}{sig}"
    except (ValueError, TypeError):
        signature = f"{name}(...)"

    # Get docstring
    docstring = inspect.getdoc(func) or "No documentation available"

    # Extract summary and example
    summary = _extract_summary(docstring)
    example = _extract_example(docstring)

    # Categorize
    category = _categorize_operator(name, docstring)

    return OperatorInfo(
        name=name,
        function=func,
        signature=signature,
        summary=summary,
        docstring=docstring,
        category=category,
        example=example,
    )


def search_operators(query: str) -> List[Tuple[str, str]]:
    """Search for operators by name or description.

    Args:
        query: Search query (case-insensitive).

    Returns:
        List of matching (name, summary) tuples.

    Example:
        ```python
        # Find all window-related operators
        window_ops = search_operators('window')

        # Find operators that work with keys
        key_ops = search_operators('key')
        ```

    """
    query_lower = query.lower()
    all_ops = list_operators()

    matching = []
    for name, summary in all_ops:
        # Check if query matches name or summary
        if query_lower in name.lower() or query_lower in summary.lower():
            matching.append((name, summary))

    return matching


def list_categories() -> List[str]:
    """List all operator categories.

    Returns:
        List of category names.

    Example:
        ```python
        categories = list_categories()
        for cat in categories:
            print(f"{cat}:")
            ops = list_operators(category=cat)
            for name, summary in ops:
                print(f"  - {name}")
        ```

    """
    # Get all operators and their categories
    all_ops = []
    for name in dir(op):
        if name.startswith("_"):
            continue
        attr = getattr(op, name)
        if callable(attr):
            docstring = inspect.getdoc(attr)
            category = _categorize_operator(name, docstring)
            all_ops.append(category)

    # Return unique categories, sorted
    return sorted(set(all_ops))


def print_operator_help(name: str) -> None:
    """Print detailed help for an operator.

    This is a convenience function for displaying operator information
    in a formatted way.

    Args:
        name: Operator name.

    Example:
        ```python
        print_operator_help('map')
        ```

    """
    info = describe_operator(name)

    print(f"\n{'='*60}")
    print(f"Operator: {info.name}")
    print(f"{'='*60}")
    print(f"\nCategory: {info.category}")
    print(f"\nSignature:")
    print(f"  {info.signature}")
    print(f"\nSummary:")
    print(f"  {info.summary}")

    if info.example:
        print(f"\nExample:")
        print("```python")
        print(info.example)
        print("```")

    print(f"\nFull Documentation:")
    print(info.docstring)
    print(f"\n{'='*60}\n")


# CLI main function
def main() -> None:
    """CLI entry point for operator discovery.

    Usage:
        python -m bytewax.discovery list [--category CATEGORY]
        python -m bytewax.discovery describe OPERATOR
        python -m bytewax.discovery search QUERY
        python -m bytewax.discovery categories

    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Bytewax operator discovery tool",
        prog="python -m bytewax.discovery",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # List command
    list_parser = subparsers.add_parser("list", help="List all operators")
    list_parser.add_argument(
        "--category", "-c", help="Filter by category", default=None
    )

    # Describe command
    describe_parser = subparsers.add_parser(
        "describe", help="Describe an operator in detail"
    )
    describe_parser.add_argument("operator", help="Operator name")

    # Search command
    search_parser = subparsers.add_parser("search", help="Search for operators")
    search_parser.add_argument("query", help="Search query")

    # Categories command
    subparsers.add_parser("categories", help="List all categories")

    args = parser.parse_args()

    if args.command == "list":
        operators = list_operators(category=args.category)
        if args.category:
            print(f"\n{args.category.title()} Operators:")
        else:
            print("\nAll Operators:")
        print("=" * 60)
        for name, summary in operators:
            print(f"  {name:20s} - {summary}")
        print()

    elif args.command == "describe":
        print_operator_help(args.operator)

    elif args.command == "search":
        results = search_operators(args.query)
        print(f"\nSearch results for '{args.query}':")
        print("=" * 60)
        if results:
            for name, summary in results:
                print(f"  {name:20s} - {summary}")
        else:
            print("  No operators found.")
        print()

    elif args.command == "categories":
        categories = list_categories()
        print("\nOperator Categories:")
        print("=" * 60)
        for cat in categories:
            ops = list_operators(category=cat)
            print(f"\n{cat.title()} ({len(ops)} operators):")
            for name, summary in ops[:5]:  # Show first 5
                print(f"  - {name}")
            if len(ops) > 5:
                print(f"  ... and {len(ops) - 5} more")
        print()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
