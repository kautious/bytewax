"""Tests for debugging utilities."""

import time

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.debug import (
    OperatorStats,
    StreamCounter,
    StreamSampler,
    capture_stream,
    clear_operator_stats,
    get_operator_stats,
    profile_function,
)
from bytewax.testing import TestingSink, TestingSource, run_main


def test_stream_sampler_basic():
    """Test basic stream sampling."""
    sampler = StreamSampler(max_samples=10)
    out = []

    flow = Dataflow("test_sampler")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    s = sampler.attach("test", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Check samples were captured
    samples = sampler.get_samples("test")
    assert len(samples) == 5
    assert samples == [1, 2, 3, 4, 5]

    # Output should be unchanged
    assert out == [1, 2, 3, 4, 5]


def test_stream_sampler_max_samples():
    """Test that max_samples limit is respected."""
    sampler = StreamSampler(max_samples=3)
    out = []

    flow = Dataflow("test_max")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    s = sampler.attach("test", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Only first 3 should be sampled
    samples = sampler.get_samples("test")
    assert len(samples) == 3
    assert samples == [1, 2, 3]

    # All items should still flow through
    assert out == [1, 2, 3, 4, 5]


def test_stream_sampler_multiple_streams():
    """Test sampling multiple streams."""
    sampler = StreamSampler()
    out = []

    flow = Dataflow("test_multi")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = sampler.attach("after_input", s)
    s = op.map("double", s, lambda x: x * 2)
    s = sampler.attach("after_double", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Check both sampling points
    input_samples = sampler.get_samples("after_input")
    double_samples = sampler.get_samples("after_double")

    assert input_samples == [1, 2, 3]
    assert double_samples == [2, 4, 6]


def test_stream_sampler_get_count():
    """Test getting sample count."""
    sampler = StreamSampler()
    out = []

    flow = Dataflow("test_count")
    s = op.input("inp", flow, TestingSource(range(10)))
    s = sampler.attach("test", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)

    assert sampler.get_count("test") == 10


def test_stream_sampler_clear():
    """Test clearing samples."""
    sampler = StreamSampler()
    out = []

    flow = Dataflow("test_clear")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = sampler.attach("test", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)

    assert sampler.get_count("test") == 3

    # Clear samples
    sampler.clear("test")
    assert sampler.get_count("test") == 0


def test_stream_sampler_clear_all():
    """Test clearing all samples."""
    sampler = StreamSampler()
    out = []

    flow = Dataflow("test_clear_all")
    s1 = op.input("inp1", flow, TestingSource([1, 2]))
    s2 = op.input("inp2", flow, TestingSource([3, 4]))
    s1 = sampler.attach("s1", s1)
    s2 = sampler.attach("s2", s2)
    merged = op.merge("merge", s1, s2)
    op.output("out", merged, TestingSink(out))

    run_main(flow)

    assert sampler.get_count("s1") > 0
    assert sampler.get_count("s2") > 0

    # Clear all
    sampler.clear()
    assert sampler.get_count("s1") == 0
    assert sampler.get_count("s2") == 0


def test_stream_sampler_list_streams():
    """Test listing sampling points."""
    sampler = StreamSampler()
    out = []

    flow = Dataflow("test_list")
    s1 = op.input("inp1", flow, TestingSource([1]))
    s2 = op.input("inp2", flow, TestingSource([2]))
    s1 = sampler.attach("point1", s1)
    s2 = sampler.attach("point2", s2)
    merged = op.merge("merge", s1, s2)
    op.output("out", merged, TestingSink(out))

    run_main(flow)

    streams = sampler.list_streams()
    assert "point1" in streams
    assert "point2" in streams


def test_stream_sampler_with_limit():
    """Test getting samples with limit."""
    sampler = StreamSampler()
    out = []

    flow = Dataflow("test_limit")
    s = op.input("inp", flow, TestingSource(range(10)))
    s = sampler.attach("test", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Get only first 3
    samples = sampler.get_samples("test", limit=3)
    assert len(samples) == 3
    assert samples == [0, 1, 2]


def test_capture_stream():
    """Test capture_stream convenience function."""
    out = []

    flow = Dataflow("test_capture")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    s, captured = capture_stream("debug", s, max_items=3)
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Check captured items
    assert len(captured) == 3
    assert captured == [1, 2, 3]

    # Output unchanged
    assert out == [1, 2, 3, 4, 5]


def test_profile_function():
    """Test function profiling."""
    # Clear any previous stats
    clear_operator_stats()

    out = []

    @profile_function("slow_transform")
    def slow_transform(x):
        time.sleep(0.001)  # 1ms
        return x * 2

    flow = Dataflow("test_profile")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = op.map("transform", s, slow_transform)
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Check stats were recorded
    stats = get_operator_stats("slow_transform")
    assert "slow_transform" in stats

    stat = stats["slow_transform"]
    assert stat.call_count == 3
    assert stat.total_time > 0
    assert stat.min_time > 0
    assert stat.max_time > 0
    assert stat.avg_time > 0


def test_operator_stats_record():
    """Test OperatorStats recording."""
    stats = OperatorStats("test_op")

    assert stats.call_count == 0
    assert stats.total_time == 0.0
    assert stats.avg_time == 0.0

    # Record some times
    stats.record(0.1)
    stats.record(0.2)
    stats.record(0.3)

    assert stats.call_count == 3
    assert stats.total_time == 0.6
    assert stats.avg_time == 0.2
    assert stats.min_time == 0.1
    assert stats.max_time == 0.3


def test_get_operator_stats_all():
    """Test getting all operator stats."""
    clear_operator_stats()

    @profile_function("op1")
    def op1(x):
        return x

    @profile_function("op2")
    def op2(x):
        return x * 2

    # Call functions
    op1(1)
    op2(2)

    all_stats = get_operator_stats()
    assert "op1" in all_stats
    assert "op2" in all_stats


def test_clear_operator_stats_specific():
    """Test clearing specific operator stats."""
    clear_operator_stats()

    @profile_function("op1")
    def op1(x):
        return x

    @profile_function("op2")
    def op2(x):
        return x

    op1(1)
    op2(2)

    # Clear op1
    clear_operator_stats("op1")

    stats = get_operator_stats()
    assert "op1" not in stats
    assert "op2" in stats


def test_clear_operator_stats_all():
    """Test clearing all operator stats."""
    clear_operator_stats()

    @profile_function("op1")
    def op1(x):
        return x

    op1(1)

    assert len(get_operator_stats()) > 0

    clear_operator_stats()
    assert len(get_operator_stats()) == 0


def test_stream_counter_basic():
    """Test basic stream counting."""
    counter = StreamCounter()
    out = []

    flow = Dataflow("test_counter")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    s = counter.attach("test", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)

    assert counter.get_count("test") == 5
    assert out == [1, 2, 3, 4, 5]


def test_stream_counter_multiple_points():
    """Test counting at multiple points."""
    counter = StreamCounter()
    out = []

    flow = Dataflow("test_multi_count")
    s = op.input("inp", flow, TestingSource([1, 2, 3, 4, 5]))
    s = counter.attach("input", s)
    s = op.filter("evens", s, lambda x: x % 2 == 0)
    s = counter.attach("after_filter", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)

    assert counter.get_count("input") == 5
    assert counter.get_count("after_filter") == 2


def test_stream_counter_reset_specific():
    """Test resetting specific counter."""
    counter = StreamCounter()
    out = []

    flow = Dataflow("test_reset")
    s1 = op.input("inp1", flow, TestingSource([1, 2]))
    s2 = op.input("inp2", flow, TestingSource([3, 4]))
    s1 = counter.attach("s1", s1)
    s2 = counter.attach("s2", s2)
    merged = op.merge("merge", s1, s2)
    op.output("out", merged, TestingSink(out))

    run_main(flow)

    assert counter.get_count("s1") > 0
    assert counter.get_count("s2") > 0

    # Reset s1
    counter.reset("s1")
    assert counter.get_count("s1") == 0
    assert counter.get_count("s2") > 0


def test_stream_counter_reset_all():
    """Test resetting all counters."""
    counter = StreamCounter()
    out = []

    flow = Dataflow("test_reset_all")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = counter.attach("test", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)

    assert counter.get_count("test") == 3

    counter.reset()
    assert counter.get_count("test") == 0


def test_stream_sampler_does_not_affect_output():
    """Test that sampling doesn't modify stream."""
    sampler = StreamSampler()
    out_with_sampler = []
    out_without_sampler = []

    # With sampler
    flow1 = Dataflow("with_sampler")
    s1 = op.input("inp", flow1, TestingSource([1, 2, 3]))
    s1 = sampler.attach("test", s1)
    op.output("out", s1, TestingSink(out_with_sampler))
    run_main(flow1)

    # Without sampler
    flow2 = Dataflow("without_sampler")
    s2 = op.input("inp", flow2, TestingSource([1, 2, 3]))
    op.output("out", s2, TestingSink(out_without_sampler))
    run_main(flow2)

    # Outputs should be identical
    assert out_with_sampler == out_without_sampler


def test_profile_function_with_errors():
    """Test profiling when function raises error."""
    clear_operator_stats()

    @profile_function("error_func")
    def error_func(x):
        if x == 2:
            raise ValueError("Error at 2")
        return x

    out = []

    flow = Dataflow("test_error")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = op.map("error", s, error_func)
    op.output("out", s, TestingSink(out))

    try:
        run_main(flow)
    except ValueError:
        pass

    # Stats should still be recorded for successful calls
    stats = get_operator_stats("error_func")
    assert "error_func" in stats
    # At least first item should be counted
    assert stats["error_func"].call_count >= 1


def test_stream_sampler_with_complex_types():
    """Test sampling with complex data types."""
    sampler = StreamSampler()
    out = []

    complex_data = [
        {"id": 1, "value": [1, 2, 3]},
        {"id": 2, "value": [4, 5, 6]},
    ]

    flow = Dataflow("test_complex")
    s = op.input("inp", flow, TestingSource(complex_data))
    s = sampler.attach("test", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)

    samples = sampler.get_samples("test")
    assert len(samples) == 2
    assert samples[0]["id"] == 1
    assert samples[1]["id"] == 2


def test_capture_stream_with_transforms():
    """Test capture_stream with transformations."""
    out = []

    flow = Dataflow("test_capture_transform")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s, captured_before = capture_stream("before", s)
    s = op.map("double", s, lambda x: x * 2)
    s, captured_after = capture_stream("after", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)

    assert captured_before == [1, 2, 3]
    assert captured_after == [2, 4, 6]
    assert out == [2, 4, 6]
