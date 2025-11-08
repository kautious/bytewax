"""Helper functions for using operators."""

import random
from datetime import timedelta
from typing import Callable, Dict, Optional, Set, Tuple, TypeVar

K = TypeVar("K")
V = TypeVar("V")
X = TypeVar("X")


def map_dict_value(
    key: K, mapper: Callable[[V], V]
) -> Callable[[Dict[K, V]], Dict[K, V]]:
    """Build a function to map an item in a dict and return the dict.

    Use this to help build mapper functions for the
    {py:obj}`~bytewax.operators.map` operator that work on a specific
    value in a dict, but leave the other values untouched.

    ```{testcode}
    import bytewax.operators as op
    from bytewax.testing import TestingSource
    from bytewax.dataflow import Dataflow
    from bytewax.operators.helpers import map_dict_value

    flow = Dataflow("lens_item_map_eg")
    s = op.input(
        "inp",
        flow,
        TestingSource(
            [
                {"name": "Rachel White", "email": "rachel@white.com"},
                {"name": "John Smith", "email": "john@smith.com"},
            ]
        ),
    )

    def normalize(name):
        return name.upper()

    s = op.map("normalize", s, map_dict_value("name", normalize))

    _ = op.inspect("out", s)
    ```

    ```{testcode}
    :hide:

    from bytewax.testing import run_main

    run_main(flow)
    ```

    ```{testoutput}
    lens_item_map_eg.out: {'name': 'RACHEL WHITE', 'email': 'rachel@white.com'}
    lens_item_map_eg.out: {'name': 'JOHN SMITH', 'email': 'john@smith.com'}
    ```

    This type of "do an operation on a known nested structure" is
    called a **lens**. If you'd like to produce more complex lenses,
    see the [`lenses`](https://github.com/ingolemo/python-lenses)
    package. It handles many more nuances of this problem like mutable
    vs immutable data types, attributes vs keys, and mutating methods
    vs returning functions. You can use it to build mappers for
    Bytewax operators.


    :arg key: Dictionary key.

    :arg mapper: Function to run on the value for that key.

    :returns: A function which will perform that mapping operation
        when called.

    """

    def shim_mapper(obj: Dict[K, V]) -> Dict[K, V]:
        obj[key] = mapper(obj[key])
        return obj

    return shim_mapper


def deduplicate(step_id: str, stream, key_fn: Optional[Callable[[X], str]] = None):
    """Remove duplicate items from stream.

    This operator uses stateful processing to track seen items and
    filter out duplicates.

    Args:
        step_id: Unique step ID.
        stream: Input stream.
        key_fn: Optional function to extract deduplication key.
                If None, uses the item itself.

    Returns:
        Stream with duplicates removed.

    Example:
        ```python
        import bytewax.operators as op
        from bytewax.operators.helpers import deduplicate

        # Remove duplicates
        s = deduplicate("dedup", stream)

        # Deduplicate by specific field
        s = deduplicate("dedup", stream, key_fn=lambda x: x["id"])
        ```

    Note:
        This keeps state for all seen items. For streams with many unique
        items, consider using a time-based window instead.

    """
    import bytewax.operators as op

    # If no key function, key by the item itself (converted to string)
    if key_fn is None:
        keyed = op.key_on(f"{step_id}_key_on", stream, lambda x: str(x))
    else:
        keyed = op.key_on(f"{step_id}_key_on", stream, key_fn)

    # Use stateful_map to track seen keys
    def dedup_mapper(state: Optional[bool], value: X) -> Tuple[bool, Optional[X]]:
        if state is None:
            # First time seeing this key
            return (True, value)
        else:
            # Already seen this key, filter it out
            return (True, None)

    deduped = op.stateful_map(f"{step_id}_stateful", keyed, dedup_mapper)

    # Filter out None values
    result = op.filter_map(f"{step_id}_filter", deduped, lambda kv: kv[1])

    return result


def sample(step_id: str, stream, rate: float = 0.1, seed: Optional[int] = None):
    """Sample items from stream with given rate.

    Args:
        step_id: Unique step ID.
        stream: Input stream.
        rate: Sampling rate (0.0 to 1.0). Default 0.1 (10%).
        seed: Optional random seed for reproducibility.

    Returns:
        Sampled stream (approximately rate * input_size items).

    Example:
        ```python
        from bytewax.operators.helpers import sample

        # Sample 10% of items
        s = sample("sample", stream, rate=0.1)

        # Sample 50% with fixed seed
        s = sample("sample", stream, rate=0.5, seed=42)
        ```

    """
    import bytewax.operators as op

    if seed is not None:
        random.seed(seed)

    def should_keep(x):
        return random.random() < rate

    return op.filter(step_id, stream, should_keep)


def take(step_id: str, stream, n: int):
    """Take first n items from stream.

    Args:
        step_id: Unique step ID.
        stream: Input stream.
        n: Number of items to take.

    Returns:
        Stream with at most n items.

    Example:
        ```python
        from bytewax.operators.helpers import take

        # Take first 100 items
        s = take("first_100", stream, 100)
        ```

    Note:
        This uses stateful processing to track count across the stream.

    """
    import bytewax.operators as op

    # Key everything to single key to maintain global count
    keyed = op.key_on(f"{step_id}_key", stream, lambda x: "global")

    # Track count and filter
    def take_mapper(state: Optional[int], value):
        count = state if state is not None else 0
        if count < n:
            return (count + 1, value)
        else:
            return (count, None)

    taken = op.stateful_map(f"{step_id}_stateful", keyed, take_mapper)

    # Filter out None values
    result = op.filter_map(f"{step_id}_filter", taken, lambda kv: kv[1])

    return result


def tee(step_id: str, stream):
    """Duplicate a stream for multiple processing paths.

    This is a convenience function that simply returns the same stream
    twice, which can then be used in different operations.

    Args:
        step_id: Unique step ID (not actually used, for consistency).
        stream: Input stream.

    Returns:
        Tuple of (stream, stream) - both are the same stream.

    Example:
        ```python
        from bytewax.operators.helpers import tee

        s1, s2 = tee("split", stream)
        # Process s1 one way
        evens = op.filter("evens", s1, lambda x: x % 2 == 0)
        # Process s2 another way
        odds = op.filter("odds", s2, lambda x: x % 2 == 1)
        ```

    """
    return stream, stream


def default_value(step_id: str, stream, default):
    """Replace None values with default value.

    Args:
        step_id: Unique step ID.
        stream: Input stream.
        default: Default value to use for None items.

    Returns:
        Stream with None values replaced.

    Example:
        ```python
        from bytewax.operators.helpers import default_value

        # Replace None with 0
        s = default_value("fill_none", stream, default=0)
        ```

    """
    import bytewax.operators as op

    def fill_none(x):
        return default if x is None else x

    return op.map(step_id, stream, fill_none)
