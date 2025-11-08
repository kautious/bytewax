# Bytewax Troubleshooting Guide

Common problems and solutions for Bytewax dataflow development.

## 📚 Table of Contents

- [Dataflow Structure Issues](#dataflow-structure-issues)
- [Operator Errors](#operator-errors)
- [State Management Issues](#state-management-issues)
- [Performance Problems](#performance-problems)
- [Data Quality Issues](#data-quality-issues)
- [Runtime Errors](#runtime-errors)
- [Debugging Techniques](#debugging-techniques)

---

## Dataflow Structure Issues

### Problem: "Dataflow has no input operators"

**Symptoms:**
```
ValidationError: Dataflow 'my_flow' has no input operators
```

**Cause:** Dataflow doesn't have an `op.input()` call

**Solution:**
```python
# ❌ Wrong - no input
flow = Dataflow("my_flow")
s = op.map("transform", ???, lambda x: x * 2)  # Where does data come from?

# ✅ Correct - add input
flow = Dataflow("my_flow")
s = op.input("inp", flow, source)  # Define data source
s = op.map("transform", s, lambda x: x * 2)
```

**Prevention:** Use `validate_dataflow()` before running:
```python
from bytewax.validation import validate_or_raise
validate_or_raise(flow)
```

---

### Problem: "Dataflow has no output operators"

**Symptoms:**
```
ValidationError: Dataflow 'my_flow' has no output operators
```

**Cause:** Dataflow doesn't have an `op.output()` call

**Solution:**
```python
# ❌ Wrong - no output
flow = Dataflow("my_flow")
s = op.input("inp", flow, source)
s = op.map("transform", s, lambda x: x * 2)  # Results go nowhere

# ✅ Correct - add output
flow = Dataflow("my_flow")
s = op.input("inp", flow, source)
s = op.map("transform", s, lambda x: x * 2)
op.output("out", s, sink)  # Send results to sink
```

---

### Problem: Duplicate Step IDs

**Symptoms:**
```
Error: Step ID 'transform' already exists
```

**Cause:** Using the same step_id twice in the dataflow

**Solution:**
```python
# ❌ Wrong - duplicate IDs
s = op.map("transform", s, lambda x: x * 2)
s = op.map("transform", s, lambda x: x + 1)  # Error!

# ✅ Correct - unique IDs
s = op.map("double", s, lambda x: x * 2)
s = op.map("increment", s, lambda x: x + 1)
```

**Best Practice:** Use descriptive, unique step IDs:
```python
s = op.map("parse_json", s, json.loads)
s = op.map("extract_user_id", s, lambda x: x["user_id"])
s = op.map("normalize_id", s, str.lower)
```

---

## Operator Errors

### Problem: "Expected tuple for keyed stream"

**Symptoms:**
```
TypeError: Expected (key, value) tuple, got <type>
```

**Cause:** Operator expects keyed stream but receiving unkeyed data

**Solution:**
```python
# ❌ Wrong - reduce needs keyed stream
s = op.input("nums", flow, TestingSource([1, 2, 3]))
s = op.reduce_final("sum", s, lambda acc, x: acc + x)  # Error!

# ✅ Correct - add keys first
s = op.input("nums", flow, TestingSource([1, 2, 3]))
s = op.key_on("add_key", s, lambda x: "total")  # Convert to (key, value)
s = op.reduce_final("sum", s, lambda acc, x: acc + x)
```

**Which Operators Need Keys:**
- `reduce_final`, `reduce_window`
- `fold_final`, `fold_window`
- `stateful_map`, `stateful_flat_map`
- `join`
- `map_value`, `filter_value`

**Use `key_on` to add keys:**
```python
# Add same key to all items
s = op.key_on("global_key", s, lambda x: "all")

# Add different keys
s = op.key_on("by_user", s, lambda x: x["user_id"])
```

---

### Problem: Mapper Function Returns Wrong Type

**Symptoms:**
```
TypeError: stateful_map expected (state, value) tuple, got <type>
```

**Cause:** Stateful mapper not returning tuple

**Solution:**
```python
# ❌ Wrong - returns only new value
def bad_mapper(state, value):
    return value * 2  # Missing state!

# ✅ Correct - returns (state, output)
def good_mapper(state, value):
    if state is None:
        state = 0
    new_state = state + value
    return (new_state, new_state)  # (state, output)

s = op.stateful_map("accumulate", s, good_mapper)
```

**Remember the signature:**
```python
def stateful_mapper(state: Optional[S], value: V) -> Tuple[S, O]:
    # state: previous state (None if first time)
    # value: current input value
    # returns: (new_state, output_value)
    pass
```

---

### Problem: "None is not iterable" in flat_map

**Symptoms:**
```
TypeError: 'NoneType' object is not iterable
```

**Cause:** flat_map function returning None instead of iterable

**Solution:**
```python
# ❌ Wrong - can't iterate None
def bad_splitter(x):
    if x:
        return x.split()
    return None  # Error!

s = op.flat_map("split", s, bad_splitter)

# ✅ Correct - always return iterable
def good_splitter(x):
    if x:
        return x.split()
    return []  # Empty list, not None

s = op.flat_map("split", s, good_splitter)
```

**flat_map must return:**
- List: `[item1, item2, ...]`
- Tuple: `(item1, item2, ...)`
- Generator: `(item for item in collection)`
- Empty: `[]` or `()` (not `None`)

---

## State Management Issues

### Problem: State Growing Too Large

**Symptoms:**
- Memory usage increasing over time
- Slow performance
- Out of memory errors

**Cause:** Unbounded state accumulation

**Solution:**
```python
# ❌ Wrong - state grows forever
def bad_accumulator(state, value):
    if state is None:
        state = []
    state.append(value)  # Keeps growing!
    return (state, state)

# ✅ Correct - limit state size
def good_accumulator(state, value, max_size=1000):
    if state is None:
        state = []
    state.append(value)
    state = state[-max_size:]  # Keep only recent items
    return (state, state)
```

**Best Practices:**
- Set maximum size for collections
- Use windowing for time-based limits
- Periodically clear old data
- Use sets for deduplication (more memory efficient)

---

### Problem: State Not Initializing

**Symptoms:**
```
AttributeError: 'NoneType' object has no attribute 'append'
```

**Cause:** Forgetting to handle `state is None` case

**Solution:**
```python
# ❌ Wrong - assumes state exists
def bad_mapper(state, value):
    state.append(value)  # Crashes if state is None!
    return (state, state)

# ✅ Correct - check and initialize
def good_mapper(state, value):
    if state is None:
        state = []  # Initialize on first call
    state.append(value)
    return (state, state)
```

**Pattern:**
```python
def stateful_func(state, value):
    # Always check and initialize state
    if state is None:
        state = <initial_value>  # dict, list, set, 0, etc.

    # Now safe to use state
    # ... your logic ...

    return (new_state, output)
```

---

## Performance Problems

### Problem: Slow Processing

**Symptoms:**
- Dataflow takes a long time to process
- High CPU usage
- Lagging behind input rate

**Diagnosis:**
```python
from bytewax.debug import profile_function

@profile_function("slow_op", slow_threshold_ms=100)
def potentially_slow(x):
    return expensive_operation(x)

s = op.map("transform", s, potentially_slow)

# After running, check stats
print(f"Avg: {potentially_slow.stats.avg_time_ms:.2f}ms")
print(f"Max: {potentially_slow.stats.max_time_ms:.2f}ms")
```

**Solutions:**

1. **Optimize expensive operations:**
```python
# ❌ Slow - regex compilation in loop
def slow_parse(text):
    import re
    pattern = re.compile(r'\d+')  # Compiled every call!
    return pattern.findall(text)

# ✅ Fast - compile once
import re
pattern = re.compile(r'\d+')

def fast_parse(text):
    return pattern.findall(text)
```

2. **Batch operations:**
```python
# Process items in batches
s = op.collect("batch", s, max_size=100)
s = op.map("batch_process", s, process_batch)
s = op.flatten("flatten", s)
```

3. **Reduce state size:**
```python
# Keep only what you need
def efficient_state(state, value):
    if state is None:
        state = {"count": 0, "sum": 0}  # Not full list

    state["count"] += 1
    state["sum"] += value
    # Don't keep all values!

    return (state, state["sum"] / state["count"])
```

---

### Problem: Memory Usage Too High

**Symptoms:**
- Process using too much RAM
- Out of memory errors
- Swapping/thrashing

**Solutions:**

1. **Limit state size:**
```python
from bytewax.operators.helpers import take

# Limit items processed
s = take("limit", s, n=10000)
```

2. **Use sampling:**
```python
from bytewax.operators.helpers import sample

# Process subset for testing
s = sample("sample", s, rate=0.1)  # 10%
```

3. **Clear old state:**
```python
def bounded_state(state, value, max_items=1000):
    if state is None:
        state = set()

    state.add(value)

    # Periodically clear
    if len(state) > max_items:
        state.clear()  # or state = state[-max_items:]

    return (state, value)
```

---

## Data Quality Issues

### Problem: None Values Causing Errors

**Symptoms:**
```
TypeError: unsupported operand type(s) for +: 'int' and 'NoneType'
```

**Solution:**
```python
from bytewax.operators.helpers import default_value

# Replace None with default
s = default_value("fill_none", s, default=0)

# Or filter out None
s = op.filter("remove_none", s, lambda x: x is not None)

# Or handle in mapper
def safe_add(x):
    return (x or 0) + 10
```

---

### Problem: Duplicates in Results

**Symptoms:**
- Same item appearing multiple times
- Counts higher than expected

**Solution:**
```python
from bytewax.operators.helpers import deduplicate

# Remove duplicates
s = deduplicate("dedup", s)

# Or deduplicate by field
s = deduplicate("dedup", s, key_fn=lambda x: x["id"])
```

---

### Problem: Unexpected Data Types

**Symptoms:**
```
TypeError: string indices must be integers
```

**Solution:**
```python
# Add type validation
def validate_type(x):
    if not isinstance(x, dict):
        print(f"Warning: expected dict, got {type(x)}")
        return None
    return x

s = op.map("validate", s, validate_type)
s = op.filter("valid_only", s, lambda x: x is not None)
```

---

## Runtime Errors

### Problem: "Stream doesn't have method 'map'"

**Symptoms:**
```
AttributeError: 'Stream' object has no attribute 'map'
```

**Cause:** Fluent API not enabled

**Solution:**
```python
from bytewax.fluent import add_fluent_methods

# Enable fluent methods
add_fluent_methods()

# Now you can chain
s = op.input("inp", flow, source).map("double", lambda x: x * 2)
```

Or use operators directly:
```python
# Standard approach (always works)
s = op.input("inp", flow, source)
s = op.map("double", s, lambda x: x * 2)
```

---

### Problem: Import Errors

**Symptoms:**
```
ModuleNotFoundError: No module named 'bytewax.validation'
```

**Cause:** Using new features but haven't updated code

**Solution:**
Check feature availability:
```python
# Phase 1 features
try:
    from bytewax.validation import validate_dataflow
    VALIDATION_AVAILABLE = True
except ImportError:
    VALIDATION_AVAILABLE = False

# Use feature if available
if VALIDATION_AVAILABLE:
    validate_dataflow(flow)
```

---

## Debugging Techniques

### Technique 1: Add Inspect Operators

```python
# Add inspect between operators to see data
s = op.input("inp", flow, source)
s = op.inspect("after_input", s, lambda x: print(f"Input: {x}"))

s = op.map("transform", s, transform)
s = op.inspect("after_transform", s, lambda x: print(f"Transformed: {x}"))

s = op.filter("filter", s, predicate)
s = op.inspect("after_filter", s, lambda x: print(f"Filtered: {x}"))
```

### Technique 2: Capture Samples

```python
from bytewax.debug import StreamSampler

sampler = StreamSampler(max_samples=10)
s = sampler.attach("checkpoint", s)

# After running
samples = sampler.get_samples("checkpoint")
print("Sample data:", samples)
```

### Technique 3: Count Items

```python
from bytewax.debug import StreamCounter

counter = StreamCounter()
s = counter.attach("before_filter", s)
s = op.filter("filter", s, predicate)
s = counter.attach("after_filter", s)

# After running
before = counter.get_count("before_filter")
after = counter.get_count("after_filter")
print(f"Filtered out: {before - after} items")
```

### Technique 4: Test with Small Data

```python
from bytewax.operators.helpers import take

# Test with first 100 items
s = op.input("inp", flow, source)
s = take("limit", s, n=100)  # Only process 100 items
# ... rest of pipeline
```

### Technique 5: Validate Structure

```python
from bytewax.validation import validate_dataflow

# Check for structural issues
errors, warnings = validate_dataflow(flow)

if errors:
    print("Errors:")
    for err in errors:
        print(f"  - {err}")

if warnings:
    print("Warnings:")
    for warn in warnings:
        print(f"  - {warn}")
```

---

## Quick Diagnosis Checklist

When something goes wrong, check:

- [ ] Does dataflow have input? (`op.input()`)
- [ ] Does dataflow have output? (`op.output()`)
- [ ] Are all step IDs unique?
- [ ] Do keyed operators receive `(key, value)` tuples?
- [ ] Do stateful mappers return `(state, output)` tuples?
- [ ] Does flat_map return iterables (not None)?
- [ ] Is state initialized (handle `state is None`)?
- [ ] Are there None values breaking operations?
- [ ] Is data type what operators expect?
- [ ] Have you validated the dataflow structure?

---

## Getting More Help

### Use Operator Discovery

```bash
# List all operators
python -m bytewax.discovery list

# Search for specific operators
python -m bytewax.discovery search "window"

# Get detailed info
python -m bytewax.discovery describe map
```

### Enable Debug Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Check Examples

See the `examples/` directory for working examples of common patterns.

### Read the Documentation

- **Pattern Cookbook**: `PATTERN_COOKBOOK.md` - Common patterns
- **Implementation Summary**: `IMPLEMENTATION_SUMMARY.md` - Feature overview
- **Examples**: `examples/README.md` - Progressive examples

---

## Common Error Messages Quick Reference

| Error | Likely Cause | Solution |
|-------|--------------|----------|
| "No input operators" | Missing `op.input()` | Add input operator |
| "No output operators" | Missing `op.output()` | Add output operator |
| "Duplicate step ID" | Reused step_id | Use unique step IDs |
| "Expected tuple" | Keyed operator without keys | Use `op.key_on()` first |
| "None not iterable" | flat_map returns None | Return `[]` instead of None |
| "State None has no attribute" | State not initialized | Check `if state is None` |
| "'Stream' has no attribute" | Fluent API not enabled | Use `add_fluent_methods()` |

---

*Can't find your issue? Check the examples directory or file an issue on GitHub.*
