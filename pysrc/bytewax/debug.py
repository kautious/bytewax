"""Debugging utilities for dataflow development.

This module provides tools to help debug dataflows during development,
including stream sampling, operator profiling, and execution tracing.

Example usage:

```python
from bytewax.debug import StreamSampler, capture_stream
import bytewax.operators as op
from bytewax.dataflow import Dataflow

# Sample streams for debugging
sampler = StreamSampler()
flow = Dataflow("debug")
s = op.input("inp", flow, source)
s = sampler.attach("after_input", s)
s = op.map("transform", s, transform_fn)
s = sampler.attach("after_transform", s)

run_main(flow)

# View samples
print("After input:", sampler.get_samples("after_input", limit=5))
print("After transform:", sampler.get_samples("after_transform", limit=5))
```

"""

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import bytewax.operators as op
from bytewax.dataflow import Stream


@dataclass
class StreamSampler:
    """Sample items from streams for debugging.

    This class allows you to capture a sample of items flowing through
    streams without affecting the dataflow execution.

    Example:
        ```python
        sampler = StreamSampler(max_samples=100)

        flow = Dataflow("debug")
        s = op.input("inp", flow, source)
        s = sampler.attach("after_input", s)
        s = op.map("transform", s, fn)

        run_main(flow)

        # View captured samples
        samples = sampler.get_samples("after_input")
        print(f"Captured {len(samples)} items")
        ```

    """

    max_samples: int = 1000
    """Maximum number of samples to keep per stream."""

    _samples: Dict[str, List[Any]] = field(default_factory=dict, init=False)
    """Internal storage for samples."""

    def attach(self, name: str, stream: Stream) -> Stream:
        """Attach sampler to a stream.

        Args:
            name: Name for this sampling point.
            stream: Stream to sample from.

        Returns:
            The same stream (for chaining).

        Example:
            ```python
            s = sampler.attach("debug_point", s)
            ```

        """
        # Initialize storage for this stream
        if name not in self._samples:
            self._samples[name] = []

        # Create inspector that captures samples
        def capture(item):
            if len(self._samples[name]) < self.max_samples:
                # Store a copy to avoid mutation issues
                try:
                    import copy

                    self._samples[name].append(copy.deepcopy(item))
                except Exception:
                    # If deepcopy fails, store original
                    self._samples[name].append(item)

        # Use inspect operator to capture without modifying stream
        return op.inspect(f"{name}_sampler", stream, capture)

    def get_samples(self, name: str, limit: Optional[int] = None) -> List[Any]:
        """Get captured samples for a stream.

        Args:
            name: Name of the sampling point.
            limit: Maximum number of samples to return (default: all).

        Returns:
            List of sampled items.

        Example:
            ```python
            samples = sampler.get_samples("debug_point", limit=10)
            for item in samples:
                print(item)
            ```

        """
        samples = self._samples.get(name, [])
        if limit is not None:
            return samples[:limit]
        return samples

    def clear(self, name: Optional[str] = None) -> None:
        """Clear captured samples.

        Args:
            name: Name of sampling point to clear, or None to clear all.

        Example:
            ```python
            sampler.clear("debug_point")  # Clear specific
            sampler.clear()  # Clear all
            ```

        """
        if name is None:
            self._samples.clear()
        elif name in self._samples:
            self._samples[name].clear()

    def get_count(self, name: str) -> int:
        """Get number of samples captured.

        Args:
            name: Name of sampling point.

        Returns:
            Number of samples captured.

        Example:
            ```python
            count = sampler.get_count("debug_point")
            print(f"Captured {count} items")
            ```

        """
        return len(self._samples.get(name, []))

    def list_streams(self) -> List[str]:
        """List all sampling points.

        Returns:
            List of sampling point names.

        Example:
            ```python
            for name in sampler.list_streams():
                count = sampler.get_count(name)
                print(f"{name}: {count} samples")
            ```

        """
        return list(self._samples.keys())


def capture_stream(
    step_id: str, stream: Stream, max_items: int = 100
) -> tuple[Stream, List[Any]]:
    """Capture items from a stream for inspection.

    This is a convenience function that creates a list to capture items
    and returns both the stream and the capture list.

    Args:
        step_id: Unique step ID for the capture operator.
        stream: Stream to capture from.
        max_items: Maximum items to capture.

    Returns:
        Tuple of (stream, capture_list).

    Example:
        ```python
        s, captured = capture_stream("debug", s, max_items=10)
        # ... continue building dataflow
        run_main(flow)
        # Now examine captured items
        print(captured)
        ```

    """
    captured: List[Any] = []

    def capture(item):
        if len(captured) < max_items:
            try:
                import copy

                captured.append(copy.deepcopy(item))
            except Exception:
                captured.append(item)

    stream = op.inspect(step_id, stream, capture)
    return stream, captured


@dataclass
class OperatorStats:
    """Statistics for an operator.

    Attributes:
        step_id: Operator step ID.
        call_count: Number of times operator was called.
        total_time: Total execution time in seconds.
        min_time: Minimum execution time.
        max_time: Maximum execution time.
        avg_time: Average execution time.

    """

    step_id: str
    call_count: int = 0
    total_time: float = 0.0
    min_time: float = float("inf")
    max_time: float = 0.0

    @property
    def avg_time(self) -> float:
        """Average execution time."""
        if self.call_count == 0:
            return 0.0
        return self.total_time / self.call_count

    def record(self, elapsed: float) -> None:
        """Record an execution time.

        Args:
            elapsed: Execution time in seconds.

        """
        self.call_count += 1
        self.total_time += elapsed
        self.min_time = min(self.min_time, elapsed)
        self.max_time = max(self.max_time, elapsed)


_operator_stats: Dict[str, OperatorStats] = {}


def profile_function(step_id: str, func: Callable) -> Callable:
    """Wrap a function to profile its execution time.

    This is useful for profiling mapper, filter, and other user functions.

    Args:
        step_id: Identifier for this function (for stats).
        func: Function to profile.

    Returns:
        Wrapped function that records timing.

    Example:
        ```python
        from bytewax.debug import profile_function

        @profile_function("my_transform")
        def transform(x):
            return expensive_operation(x)

        # Use in dataflow
        s = op.map("transform", s, transform)

        # After running, check stats
        from bytewax.debug import get_operator_stats
        stats = get_operator_stats()
        for step_id, stat in stats.items():
            print(f"{step_id}: {stat.avg_time*1000:.2f}ms avg")
        ```

    """
    if step_id not in _operator_stats:
        _operator_stats[step_id] = OperatorStats(step_id)

    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            elapsed = time.perf_counter() - start
            _operator_stats[step_id].record(elapsed)

    return wrapper


def get_operator_stats(step_id: Optional[str] = None) -> Dict[str, OperatorStats]:
    """Get operator profiling statistics.

    Args:
        step_id: Optional step ID to get stats for specific operator.

    Returns:
        Dictionary of step_id -> OperatorStats.

    Example:
        ```python
        stats = get_operator_stats()
        for step_id, stat in stats.items():
            print(f"{step_id}:")
            print(f"  Calls: {stat.call_count}")
            print(f"  Total: {stat.total_time:.3f}s")
            print(f"  Avg: {stat.avg_time*1000:.2f}ms")
            print(f"  Min: {stat.min_time*1000:.2f}ms")
            print(f"  Max: {stat.max_time*1000:.2f}ms")
        ```

    """
    if step_id is not None:
        if step_id in _operator_stats:
            return {step_id: _operator_stats[step_id]}
        return {}
    return dict(_operator_stats)


def clear_operator_stats(step_id: Optional[str] = None) -> None:
    """Clear profiling statistics.

    Args:
        step_id: Optional step ID to clear stats for specific operator,
                 or None to clear all stats.

    Example:
        ```python
        clear_operator_stats()  # Clear all
        clear_operator_stats("my_transform")  # Clear specific
        ```

    """
    if step_id is None:
        _operator_stats.clear()
    elif step_id in _operator_stats:
        del _operator_stats[step_id]


def print_operator_stats(top_n: Optional[int] = None) -> None:
    """Print operator profiling statistics in a formatted table.

    Args:
        top_n: Show only top N slowest operators (default: all).

    Example:
        ```python
        # After running dataflow
        print_operator_stats(top_n=10)
        ```

    """
    stats = list(_operator_stats.values())

    if not stats:
        print("No profiling data available.")
        return

    # Sort by total time (slowest first)
    stats.sort(key=lambda s: s.total_time, reverse=True)

    if top_n is not None:
        stats = stats[:top_n]

    print("\nOperator Profiling Statistics")
    print("=" * 80)
    print(
        f"{'Step ID':<30} {'Calls':>8} {'Total':>10} {'Avg':>10} {'Min':>10} {'Max':>10}"
    )
    print("-" * 80)

    for stat in stats:
        print(
            f"{stat.step_id:<30} "
            f"{stat.call_count:>8} "
            f"{stat.total_time:>9.3f}s "
            f"{stat.avg_time*1000:>9.2f}ms "
            f"{stat.min_time*1000:>9.2f}ms "
            f"{stat.max_time*1000:>9.2f}ms"
        )

    print("=" * 80)
    print()


class StreamCounter:
    """Count items passing through a stream.

    This is a lightweight debugging tool that just counts items
    without storing them.

    Example:
        ```python
        counter = StreamCounter()

        flow = Dataflow("count")
        s = op.input("inp", flow, source)
        s = counter.attach("input", s)
        s = op.filter("valid", s, is_valid)
        s = counter.attach("after_filter", s)

        run_main(flow)

        print(f"Input: {counter.get_count('input')}")
        print(f"After filter: {counter.get_count('after_filter')}")
        ```

    """

    def __init__(self):
        """Create a stream counter."""
        self._counts: Dict[str, int] = {}

    def attach(self, name: str, stream: Stream) -> Stream:
        """Attach counter to a stream.

        Args:
            name: Name for this counting point.
            stream: Stream to count.

        Returns:
            The same stream (for chaining).

        """
        if name not in self._counts:
            self._counts[name] = 0

        def increment(_item):
            self._counts[name] += 1

        return op.inspect(f"{name}_counter", stream, increment)

    def get_count(self, name: str) -> int:
        """Get item count for a stream.

        Args:
            name: Name of counting point.

        Returns:
            Number of items counted.

        """
        return self._counts.get(name, 0)

    def reset(self, name: Optional[str] = None) -> None:
        """Reset counts.

        Args:
            name: Name of counting point to reset, or None for all.

        """
        if name is None:
            self._counts.clear()
        elif name in self._counts:
            self._counts[name] = 0

    def print_summary(self) -> None:
        """Print a summary of all counts."""
        print("\nStream Item Counts")
        print("=" * 50)
        for name, count in sorted(self._counts.items()):
            print(f"{name:<30} {count:>15,}")
        print("=" * 50)
        print()
