# Bytewax Examples

This directory contains progressive examples to help you learn Bytewax from beginner to advanced levels.

## 📚 Example Categories

### 🟢 Beginner Examples (`beginner/`)
Start here if you're new to Bytewax. These examples cover:
- Basic dataflow construction
- Simple transformations (map, filter)
- Input and output operations
- Working with streams

**Examples:**
- `01_hello_world.py` - Your first Bytewax dataflow
- `02_simple_transforms.py` - Basic map and filter operations
- `03_word_count.py` - Classic word count example
- `04_working_with_time.py` - Time-based data processing

### 🟡 Intermediate Examples (`intermediate/`)
Build on the basics with more complex patterns:
- Stateful processing
- Window operations
- Keyed streams
- Error handling

**Examples:**
- `01_stateful_processing.py` - Using state to track data
- `02_windowing.py` - Time and count-based windows
- `03_joins.py` - Joining multiple streams
- `04_error_handling.py` - Handling errors gracefully
- `05_custom_operators.py` - Building your own operators

### 🔴 Advanced Examples (`advanced/`)
Master advanced features and best practices:
- Dataflow validation
- Debugging and profiling
- Fluent API usage
- Helper operators
- Performance optimization

**Examples:**
- `01_validation.py` - Pre-flight validation
- `02_debugging.py` - Using debugging utilities
- `03_fluent_api.py` - Method chaining patterns
- `04_helper_operators.py` - Using helper operators
- `05_performance.py` - Performance optimization techniques
- `06_production_ready.py` - Production-ready patterns

## 🚀 Quick Start

1. **Install Bytewax:**
   ```bash
   pip install bytewax
   ```

2. **Run an example:**
   ```bash
   python examples/beginner/01_hello_world.py
   ```

3. **Progress through levels:**
   - Start with beginner examples
   - Move to intermediate when comfortable
   - Explore advanced features as needed

## 📖 Learning Path

### Path 1: Data Processing Basics
1. `beginner/01_hello_world.py`
2. `beginner/02_simple_transforms.py`
3. `beginner/03_word_count.py`
4. `intermediate/01_stateful_processing.py`

### Path 2: Time-Based Processing
1. `beginner/04_working_with_time.py`
2. `intermediate/02_windowing.py`
3. `advanced/05_performance.py`

### Path 3: Production Development
1. `intermediate/04_error_handling.py`
2. `advanced/01_validation.py`
3. `advanced/02_debugging.py`
4. `advanced/06_production_ready.py`

## 🎯 Key Concepts by Example

| Concept | Beginner | Intermediate | Advanced |
|---------|----------|--------------|----------|
| **Transformations** | 01, 02 | - | 03 |
| **State Management** | - | 01 | - |
| **Windowing** | 04 | 02 | - |
| **Validation** | - | 04 | 01 |
| **Debugging** | - | - | 02 |
| **Operators** | 02, 03 | 05 | 04 |
| **Production** | - | 04 | 06 |

## 💡 Tips for Learning

1. **Run the examples** - Don't just read them, execute and modify
2. **Read comments** - Each example has detailed inline documentation
3. **Experiment** - Try changing parameters and see what happens
4. **Progress gradually** - Don't skip ahead too quickly
5. **Check docs** - Use `python -m bytewax.discovery` to explore operators

## 🔗 Additional Resources

- **Operator Discovery**: `python -m bytewax.discovery list`
- **Describe Operator**: `python -m bytewax.discovery describe <operator>`
- **Search Operators**: `python -m bytewax.discovery search <query>`
- **Main Documentation**: See `IMPROVEMENT_PLAN.md` and `IMPLEMENTATION_SUMMARY.md`
- **Pattern Cookbook**: See `PATTERN_COOKBOOK.md`
- **Troubleshooting**: See `TROUBLESHOOTING.md`

## 📝 Example Template

When creating your own dataflows, follow this pattern:

```python
"""Brief description of what this dataflow does."""

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource, TestingSink

# 1. Create dataflow
flow = Dataflow("my_flow")

# 2. Define input
s = op.input("inp", flow, TestingSource([1, 2, 3]))

# 3. Transform data
s = op.map("transform", s, lambda x: x * 2)

# 4. Define output
op.output("out", s, TestingSink([]))

# 5. Run (in production, use run_main)
if __name__ == "__main__":
    from bytewax.testing import run_main
    run_main(flow)
```

## 🎓 Next Steps

After completing these examples:
1. Review the **Pattern Cookbook** for common patterns
2. Check the **Troubleshooting Guide** for common issues
3. Explore **Phase 1 & 2 features** (validation, debugging, helpers)
4. Build your own dataflows!

Happy learning! 🚀
