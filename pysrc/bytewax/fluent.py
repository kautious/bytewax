"""Fluent API extensions for more convenient dataflow construction.

This module provides convenience methods for building dataflows in a
more fluent/chainable style.

Example usage:

```python
from bytewax.dataflow import Dataflow
from bytewax.fluent import add_fluent_methods
import bytewax.operators as op

# Enable fluent methods
add_fluent_methods()

# Now use fluent API
flow = Dataflow("fluent_example")
result = (
    flow.input("inp", source)
        .map("parse", parse_json)
        .filter("valid", is_valid)
        .key_on("user_id", lambda x: x["user_id"])
        .map_value("extract", lambda x: x["value"])
        .reduce("sum", lambda acc, x: acc + x)
        .output("out", sink)
)
```

"""

from typing import Any, Callable, Generic, Iterable, Optional, Tuple, TypeVar

import bytewax.operators as op
from bytewax.dataflow import Dataflow, Stream
from bytewax.inputs import Source
from bytewax.outputs import Sink

X = TypeVar("X")
Y = TypeVar("Y")
V = TypeVar("V")
W = TypeVar("W")


# Store original Stream class
_original_stream_class = Stream


def add_fluent_methods() -> None:
    """Add fluent methods to Stream and Dataflow classes.

    Call this function once at the start of your program to enable
    fluent API methods.

    Example:
        ```python
        from bytewax.fluent import add_fluent_methods

        # Enable fluent methods
        add_fluent_methods()

        # Now use fluent API
        flow = Dataflow("example")
        flow.input("inp", source).map("double", lambda x: x * 2).output("out", sink)
        ```

    Note:
        This modifies the Stream and Dataflow classes by adding methods.
        Call it once before building dataflows.

    """
    # Add methods to Stream
    if not hasattr(Stream, "map"):
        Stream.map = _stream_map  # type: ignore
    if not hasattr(Stream, "filter"):
        Stream.filter = _stream_filter  # type: ignore
    if not hasattr(Stream, "flat_map"):
        Stream.flat_map = _stream_flat_map  # type: ignore
    if not hasattr(Stream, "key_on"):
        Stream.key_on = _stream_key_on  # type: ignore
    if not hasattr(Stream, "map_value"):
        Stream.map_value = _stream_map_value  # type: ignore
    if not hasattr(Stream, "filter_value"):
        Stream.filter_value = _stream_filter_value  # type: ignore
    if not hasattr(Stream, "filter_map"):
        Stream.filter_map = _stream_filter_map  # type: ignore
    if not hasattr(Stream, "filter_map_value"):
        Stream.filter_map_value = _stream_filter_map_value  # type: ignore
    if not hasattr(Stream, "inspect_debug"):
        Stream.inspect_debug = _stream_inspect  # type: ignore
    if not hasattr(Stream, "collect"):
        Stream.collect = _stream_collect  # type: ignore
    if not hasattr(Stream, "flatten"):
        Stream.flatten = _stream_flatten  # type: ignore
    if not hasattr(Stream, "output"):
        Stream.output = _stream_output  # type: ignore

    # Add methods to Dataflow
    if not hasattr(Dataflow, "input"):
        Dataflow.input = _dataflow_input  # type: ignore


# Stream fluent methods


def _stream_map(self: Stream[X], step_id: str, mapper: Callable[[X], Y]) -> Stream[Y]:
    """Map each item 1-to-1.

    Args:
        step_id: Unique step ID.
        mapper: Function to transform each item.

    Returns:
        Transformed stream.

    """
    return op.map(step_id, self, mapper)


def _stream_filter(
    self: Stream[X], step_id: str, predicate: Callable[[X], bool]
) -> Stream[X]:
    """Filter items by predicate.

    Args:
        step_id: Unique step ID.
        predicate: Function that returns True to keep item.

    Returns:
        Filtered stream.

    """
    return op.filter(step_id, self, predicate)


def _stream_flat_map(
    self: Stream[X], step_id: str, mapper: Callable[[X], Iterable[Y]]
) -> Stream[Y]:
    """Map each item to multiple items.

    Args:
        step_id: Unique step ID.
        mapper: Function that returns iterable of items.

    Returns:
        Flattened stream.

    """
    return op.flat_map(step_id, self, mapper)


def _stream_key_on(
    self: Stream[X], step_id: str, key_fn: Callable[[X], str]
) -> Stream[Tuple[str, X]]:
    """Add keys to items.

    Args:
        step_id: Unique step ID.
        key_fn: Function to extract key from item.

    Returns:
        Keyed stream of (key, value) tuples.

    """
    return op.key_on(step_id, self, key_fn)


def _stream_map_value(
    self: Stream[Tuple[str, V]], step_id: str, mapper: Callable[[V], W]
) -> Stream[Tuple[str, W]]:
    """Map values in keyed stream.

    Args:
        step_id: Unique step ID.
        mapper: Function to transform values.

    Returns:
        Stream with mapped values.

    """
    return op.map_value(step_id, self, mapper)


def _stream_filter_value(
    self: Stream[Tuple[str, V]], step_id: str, predicate: Callable[[V], bool]
) -> Stream[Tuple[str, V]]:
    """Filter values in keyed stream.

    Args:
        step_id: Unique step ID.
        predicate: Function that returns True to keep value.

    Returns:
        Filtered keyed stream.

    """
    return op.filter_value(step_id, self, predicate)


def _stream_filter_map(
    self: Stream[X], step_id: str, mapper: Callable[[X], Optional[Y]]
) -> Stream[Y]:
    """Filter and map in one step.

    Args:
        step_id: Unique step ID.
        mapper: Function that returns item to keep, or None to filter.

    Returns:
        Filtered and mapped stream.

    """
    return op.filter_map(step_id, self, mapper)


def _stream_filter_map_value(
    self: Stream[Tuple[str, V]], step_id: str, mapper: Callable[[V], Optional[W]]
) -> Stream[Tuple[str, W]]:
    """Filter and map values in keyed stream.

    Args:
        step_id: Unique step ID.
        mapper: Function that returns value to keep, or None to filter.

    Returns:
        Filtered and mapped keyed stream.

    """
    return op.filter_map_value(step_id, self, mapper)


def _stream_inspect(
    self: Stream[X], step_id: str, inspector: Callable[[X], Any]
) -> Stream[X]:
    """Inspect items without modifying stream.

    Args:
        step_id: Unique step ID.
        inspector: Function to call on each item (e.g., print).

    Returns:
        Unchanged stream.

    """
    return op.inspect(step_id, self, inspector)


def _stream_collect(
    self: Stream[X], step_id: str, max_size: int, timeout: Optional[Any] = None
) -> Stream[list[X]]:
    """Collect items into batches.

    Args:
        step_id: Unique step ID.
        max_size: Maximum batch size.
        timeout: Optional timeout for batch collection.

    Returns:
        Stream of lists.

    """
    return op.collect(step_id, self, max_size, timeout)


def _stream_flatten(self: Stream[Iterable[X]], step_id: str) -> Stream[X]:
    """Flatten nested iterables.

    Args:
        step_id: Unique step ID.

    Returns:
        Flattened stream.

    """
    return op.flatten(step_id, self)


def _stream_output(self: Stream[X], step_id: str, sink: Sink) -> None:
    """Output stream to sink.

    Args:
        step_id: Unique step ID.
        sink: Sink to write to.

    """
    op.output(step_id, self, sink)


# Dataflow fluent methods


def _dataflow_input(
    self: Dataflow, step_id: str, source: Source[X]
) -> Stream[X]:
    """Create input stream.

    Args:
        step_id: Unique step ID.
        source: Source to read from.

    Returns:
        Input stream.

    """
    return op.input(step_id, self, source)


# Convenience functions that don't require monkey-patching


def chain(stream: Stream[X]) -> "FluentStream[X]":
    """Create a fluent stream wrapper.

    This provides an alternative to monkey-patching that wraps a stream
    in a fluent interface.

    Args:
        stream: Stream to wrap.

    Returns:
        Fluent stream wrapper.

    Example:
        ```python
        from bytewax.fluent import chain

        # Wrap stream for fluent API
        result = (
            chain(stream)
            .map("double", lambda x: x * 2)
            .filter("positive", lambda x: x > 0)
            .unwrap()
        )
        ```

    """
    return FluentStream(stream)


class FluentStream(Generic[X]):
    """Fluent wrapper for Stream.

    This provides a chainable interface without modifying the Stream class.

    Example:
        ```python
        result = (
            FluentStream(stream)
            .map("double", lambda x: x * 2)
            .filter("positive", lambda x: x > 0)
            .unwrap()
        )
        ```

    """

    def __init__(self, stream: Stream[X]):
        """Create fluent stream wrapper.

        Args:
            stream: Stream to wrap.

        """
        self._stream = stream

    def map(self, step_id: str, mapper: Callable[[X], Y]) -> "FluentStream[Y]":
        """Map items."""
        return FluentStream(op.map(step_id, self._stream, mapper))

    def filter(self, step_id: str, predicate: Callable[[X], bool]) -> "FluentStream[X]":
        """Filter items."""
        return FluentStream(op.filter(step_id, self._stream, predicate))

    def flat_map(
        self, step_id: str, mapper: Callable[[X], Iterable[Y]]
    ) -> "FluentStream[Y]":
        """Flat map items."""
        return FluentStream(op.flat_map(step_id, self._stream, mapper))

    def key_on(
        self, step_id: str, key_fn: Callable[[X], str]
    ) -> "FluentStream[Tuple[str, X]]":
        """Add keys."""
        return FluentStream(op.key_on(step_id, self._stream, key_fn))

    def inspect_debug(
        self, step_id: str, inspector: Callable[[X], Any]
    ) -> "FluentStream[X]":
        """Inspect items."""
        return FluentStream(op.inspect(step_id, self._stream, inspector))

    def collect(
        self, step_id: str, max_size: int, timeout: Optional[Any] = None
    ) -> "FluentStream[list[X]]":
        """Collect into batches."""
        return FluentStream(op.collect(step_id, self._stream, max_size, timeout))

    def output(self, step_id: str, sink: Sink) -> None:
        """Output to sink."""
        op.output(step_id, self._stream, sink)

    def unwrap(self) -> Stream[X]:
        """Get underlying stream.

        Returns:
            The wrapped stream.

        """
        return self._stream
