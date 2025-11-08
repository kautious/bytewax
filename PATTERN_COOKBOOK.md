# Bytewax Pattern Cookbook

A collection of common patterns and recipes for building Bytewax dataflows.

## 📚 Table of Contents

- [Data Transformation Patterns](#data-transformation-patterns)
- [Aggregation Patterns](#aggregation-patterns)
- [Stateful Processing Patterns](#stateful-processing-patterns)
- [Error Handling Patterns](#error-handling-patterns)
- [Debugging Patterns](#debugging-patterns)
- [Performance Patterns](#performance-patterns)
- [Production Patterns](#production-patterns)

---

## Data Transformation Patterns

### Pattern: Filter-Map-Reduce

**Use Case**: Clean, transform, and aggregate data

```python
from bytewax.dataflow import Dataflow
import bytewax.operators as op

flow = Dataflow("filter_map_reduce")
s = op.input("inp", flow, source)

# Filter: keep only valid items
s = op.filter("valid", s, lambda x: x is not None and x > 0)

# Map: transform each item
s = op.map("transform", s, lambda x: x * 2)

# Key by category
s = op.map("add_key", s, lambda x: (get_category(x), x))

# Reduce: aggregate by key
s = op.reduce_final("sum", s, lambda acc, val: acc + val)

op.output("out", s, sink)
```

**When to Use**:
- Data cleaning and transformation
- Category-based aggregation
- ETL pipelines

---

### Pattern: Split-Process-Merge

**Use Case**: Process different data types separately, then merge

```python
flow = Dataflow("split_process_merge")
s = op.input("inp", flow, source)

# Split by type
branches = op.branch("split_type", s, lambda x: x["type"] == "A")

# Process each branch
stream_a = op.map("process_a", branches.trues, process_type_a)
stream_b = op.map("process_b", branches.falses, process_type_b)

# Merge back
merged = op.merge("merge", stream_a, stream_b)

op.output("out", merged, sink)
```

**When to Use**:
- Different processing logic per data type
- Specialized transformations
- Conditional routing

---

### Pattern: Enrich with Lookup

**Use Case**: Add information from a lookup table

```python
# Prepare lookup data
lookup_table = {"user1": {"name": "Alice"}, "user2": {"name": "Bob"}}

flow = Dataflow("enrich")
s = op.input("events", flow, source)

# Enrich each event with user data
def enrich(event):
    user_id = event["user_id"]
    user_data = lookup_table.get(user_id, {})
    return {**event, **user_data}

s = op.map("enrich", s, enrich)

op.output("out", s, sink)
```

**When to Use**:
- Adding reference data
- Joining with static data
- Data enrichment

---

## Aggregation Patterns

### Pattern: Running Total

**Use Case**: Maintain a running sum/count

```python
flow = Dataflow("running_total")
s = op.input("values", flow, source)

# Key all items together for global total
s = op.key_on("global_key", s, lambda x: "total")

# Maintain running sum
def running_sum(state, value):
    if state is None:
        state = 0
    new_state = state + value
    return (new_state, new_state)

s = op.stateful_map("sum", s, running_sum)

op.output("out", s, sink)
```

**When to Use**:
- Cumulative totals
- Running averages
- Progress tracking

---

### Pattern: Top-N

**Use Case**: Find the top N items by some metric

```python
flow = Dataflow("top_n")
s = op.input("items", flow, source)

# Add scores
s = op.map("score", s, lambda x: (x, calculate_score(x)))

# Collect all items
s = op.key_on("global", s, lambda x: "all")
s = op.fold_final("collect", s, list, lambda acc, x: acc + [x])

# Sort and take top N
def get_top_n(items, n=10):
    return sorted(items, key=lambda x: x[1], reverse=True)[:n]

s = op.map_value("top_n", s, lambda items: get_top_n(items, 10))

op.output("out", s, sink)
```

**When to Use**:
- Leaderboards
- Top products/users
- Ranking systems

---

### Pattern: Group By Time Period

**Use Case**: Aggregate events by hour/day/month

```python
from datetime import datetime

flow = Dataflow("group_by_time")
s = op.input("events", flow, source)

# Extract time period as key
s = op.map("hourly_key", s, lambda e: (e["timestamp"].hour, e))

# Count per hour
s = op.map("add_count", s, lambda pair: (pair[0], 1))
s = op.reduce_final("count", s, lambda acc, x: acc + x)

op.output("out", s, sink)
```

**When to Use**:
- Time-based analytics
- Hourly/daily reports
- Traffic analysis

---

## Stateful Processing Patterns

### Pattern: Deduplication

**Use Case**: Remove duplicate items

```python
# Built-in helper (recommended)
from bytewax.operators.helpers import deduplicate

flow = Dataflow("dedup")
s = op.input("items", flow, source)
s = deduplicate("dedup", s, key_fn=lambda x: x["id"])
op.output("out", s, sink)

# Or implement manually:
def dedup_mapper(state, item):
    if state is None:
        state = set()
    item_id = get_id(item)
    if item_id in state:
        return (state, None)  # Duplicate, filter out
    else:
        state.add(item_id)
        return (state, item)  # New item, keep it
```

**When to Use**:
- Remove duplicate events
- Idempotent processing
- Event deduplication

---

### Pattern: Session Tracking

**Use Case**: Track user sessions with state

```python
flow = Dataflow("sessions")
s = op.input("events", flow, source)

# Key by user
s = op.map("key_by_user", s, lambda e: (e["user_id"], e))

# Track session state
def track_session(state, event):
    if state is None:
        state = {"events": [], "start_time": event["timestamp"]}

    state["events"].append(event)
    state["last_time"] = event["timestamp"]

    return (state, state.copy())

s = op.stateful_map("track", s, track_session)

op.output("out", s, sink)
```

**When to Use**:
- User session tracking
- Conversation tracking
- Activity monitoring

---

### Pattern: Moving Window

**Use Case**: Maintain recent N items for analysis

```python
flow = Dataflow("moving_window")
s = op.input("values", flow, source)
s = op.key_on("global", s, lambda x: "all")

def moving_average(state, value, window_size=10):
    if state is None:
        state = []

    state.append(value)
    state = state[-window_size:]  # Keep last N

    avg = sum(state) / len(state)
    return (state, avg)

s = op.stateful_map("avg", s, lambda st, val: moving_average(st, val, 10))

op.output("out", s, sink)
```

**When to Use**:
- Moving averages
- Recent activity analysis
- Trend detection

---

## Error Handling Patterns

### Pattern: Try-Parse-Filter

**Use Case**: Parse data, filter out errors

```python
flow = Dataflow("safe_parse")
s = op.input("data", flow, source)

# Try to parse, return None on error
def safe_parse(x):
    try:
        return parse(x)
    except Exception:
        return None

s = op.map("parse", s, safe_parse)
s = op.filter("valid", s, lambda x: x is not None)

op.output("out", s, sink)
```

**When to Use**:
- Parsing untrusted input
- Handling malformed data
- Data validation

---

### Pattern: Separate Error Stream

**Use Case**: Route errors to different output

```python
flow = Dataflow("error_routing")
s = op.input("data", flow, source)

# Parse and tag success/error
def try_parse(x):
    try:
        return ("success", parse(x))
    except Exception as e:
        return ("error", {"data": x, "error": str(e)})

s = op.map("parse", s, try_parse)

# Split into success and error streams
branches = op.branch("split", s, lambda x: x[0] == "success")

success = op.map("get_value", branches.trues, lambda x: x[1])
errors = op.map("get_error", branches.falses, lambda x: x[1])

op.output("success", success, success_sink)
op.output("errors", errors, error_sink)
```

**When to Use**:
- Dead letter queue
- Error logging
- Separate error handling

---

### Pattern: Default Values

**Use Case**: Replace None/errors with defaults

```python
from bytewax.operators.helpers import default_value

flow = Dataflow("defaults")
s = op.input("data", flow, source)

# Replace None with default
s = default_value("fill", s, default=0)

# Or manually:
s = op.map("fill", s, lambda x: x if x is not None else 0)

op.output("out", s, sink)
```

**When to Use**:
- Missing data handling
- Null coalescing
- Default configuration

---

## Debugging Patterns

### Pattern: Inspect At Key Points

**Use Case**: Debug data flow without modifying stream

```python
flow = Dataflow("debug_inspect")
s = op.input("data", flow, source)

# Inspect after each transformation
s = op.inspect("after_input", s, lambda x: print(f"Input: {x}"))

s = op.map("transform", s, transform_func)
s = op.inspect("after_transform", s, lambda x: print(f"Transformed: {x}"))

s = op.filter("filter", s, filter_func)
s = op.inspect("after_filter", s, lambda x: print(f"Filtered: {x}"))

op.output("out", s, sink)
```

**When to Use**:
- Debugging pipelines
- Verifying transformations
- Monitoring data flow

---

### Pattern: Sample for Testing

**Use Case**: Test pipeline with subset of data

```python
from bytewax.operators.helpers import sample, take

flow = Dataflow("test_sample")
s = op.input("data", flow, source)

# Sample 10% for testing
s = sample("sample", s, rate=0.1, seed=42)

# Or take first N items
s = take("limit", s, n=100)

op.output("out", s, sink)
```

**When to Use**:
- Testing with production data
- Quick iteration
- Load testing

---

### Pattern: Capture Samples

**Use Case**: Capture samples for inspection

```python
from bytewax.debug import StreamSampler

flow = Dataflow("capture_samples")
sampler = StreamSampler(max_samples=100)

s = op.input("data", flow, source)
s = sampler.attach("after_input", s)

s = op.map("transform", s, transform_func)
s = sampler.attach("after_transform", s)

op.output("out", s, sink)
run_main(flow)

# Inspect captured samples
print("Samples:", sampler.get_samples("after_transform"))
```

**When to Use**:
- Post-mortem debugging
- Data quality checks
- Understanding data distribution

---

## Performance Patterns

### Pattern: Profile Operators

**Use Case**: Find performance bottlenecks

```python
from bytewax.debug import profile_function

@profile_function("expensive_transform", slow_threshold_ms=100)
def expensive_operation(x):
    # Complex computation
    return process(x)

flow = Dataflow("profiled")
s = op.input("data", flow, source)
s = op.map("transform", s, expensive_operation)
op.output("out", s, sink)

run_main(flow)

# Check stats
stats = expensive_operation.stats
print(f"Avg time: {stats.avg_time_ms:.2f}ms")
print(f"Max time: {stats.max_time_ms:.2f}ms")
```

**When to Use**:
- Performance optimization
- Finding slow operations
- Capacity planning

---

### Pattern: Batch Processing

**Use Case**: Process items in batches for efficiency

```python
flow = Dataflow("batch_processing")
s = op.input("items", flow, source)

# Collect into batches
s = op.collect("batch", s, max_size=100, timeout=timedelta(seconds=5))

# Process batches
s = op.map("process_batch", s, lambda batch: process_batch(batch))

# Flatten back to items
s = op.flatten("flatten", s)

op.output("out", s, sink)
```

**When to Use**:
- Database writes
- API calls
- Network operations

---

## Production Patterns

### Pattern: Validation Pipeline

**Use Case**: Validate dataflow before deployment

```python
from bytewax.validation import validate_or_raise

flow = Dataflow("validated_pipeline")

# Build dataflow
s = op.input("data", flow, source)
s = op.map("transform", s, transform)
op.output("out", s, sink)

# Validate before running
validate_or_raise(flow)

# Now safe to run
run_main(flow)
```

**When to Use**:
- Pre-deployment checks
- CI/CD pipelines
- Development workflow

---

### Pattern: Comprehensive Monitoring

**Use Case**: Monitor all aspects of dataflow

```python
from bytewax.debug import StreamSampler, StreamCounter

flow = Dataflow("monitored_pipeline")

# Set up monitoring
sampler = StreamSampler(max_samples=1000)
counter = StreamCounter()

s = op.input("data", flow, source)
s = counter.attach("input", s)
s = sampler.attach("input_sample", s)

s = op.map("transform", s, transform)
s = counter.attach("transformed", s)

s = op.filter("valid", s, is_valid)
s = counter.attach("valid", s)

op.output("out", s, sink)
run_main(flow)

# Report metrics
print(f"Input: {counter.get_count('input')}")
print(f"Valid: {counter.get_count('valid')}")
print(f"Drop rate: {(1 - counter.get_count('valid')/counter.get_count('input')) * 100:.1f}%")
```

**When to Use**:
- Production monitoring
- Quality assurance
- SLA tracking

---

### Pattern: Graceful Error Handling

**Use Case**: Handle errors without crashing

```python
from bytewax.errors import OperatorError

flow = Dataflow("resilient_pipeline")
s = op.input("data", flow, source)

def resilient_transform(x):
    try:
        return transform(x)
    except Exception as e:
        # Log error with context
        raise OperatorError(
            step_id="transform",
            operator_name="map",
            message=f"Failed to transform {x}",
            suggestion="Check data format",
        ) from e

# This will provide better error messages
s = op.map("transform", s, resilient_transform)
op.output("out", s, sink)
```

**When to Use**:
- Production systems
- Error tracking
- Debugging

---

## Quick Reference

### Common Operator Combinations

| Pattern | Operators | Use Case |
|---------|-----------|----------|
| Filter-Map | filter → map | Clean then transform |
| Map-Reduce | map → reduce | Transform then aggregate |
| Split-Merge | branch → process → merge | Conditional processing |
| Key-Reduce | key_on → reduce | Group and aggregate |
| Collect-Process | collect → map → flatten | Batch processing |
| Dedupe-Process | deduplicate → map | Remove duplicates first |

### Helper Operators Quick Guide

| Helper | Purpose | Example |
|--------|---------|---------|
| `deduplicate` | Remove duplicates | `deduplicate("d", s, key_fn)` |
| `sample` | Random sampling | `sample("s", s, rate=0.1)` |
| `take` | Limit items | `take("t", s, n=100)` |
| `tee` | Duplicate stream | `s1, s2 = tee("t", s)` |
| `default_value` | Fill None | `default_value("f", s, 0)` |

---

## Best Practices

1. **Validate Early** - Use `validate_or_raise()` in development
2. **Debug Incrementally** - Use `inspect()` to verify each step
3. **Sample for Testing** - Use `sample()` or `take()` with real data
4. **Handle Errors** - Always handle parse/validation errors
5. **Monitor Metrics** - Track counts at key points
6. **Profile Performance** - Use `profile_function()` for slow ops
7. **Document Operators** - Give descriptive step IDs
8. **Keep State Small** - Be mindful of state size in stateful operators

---

## Additional Resources

- **Examples**: See `examples/` directory for runnable examples
- **Operator Discovery**: `python -m bytewax.discovery list`
- **API Documentation**: See `IMPLEMENTATION_SUMMARY.md`
- **Troubleshooting**: See `TROUBLESHOOTING.md`

---

*This cookbook is a living document. As you discover new patterns, consider contributing them back to the community!*
