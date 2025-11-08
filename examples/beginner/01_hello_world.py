"""Hello World - Your First Bytewax Dataflow

This is the simplest possible Bytewax dataflow. It demonstrates:
- Creating a dataflow
- Adding an input source
- Transforming data with map
- Sending output to a sink
- Running the dataflow

Concepts introduced:
- Dataflow creation
- Input operator (op.input)
- Map operator (op.map)
- Output operator (op.output)
- Testing utilities (TestingSource, TestingSink)
"""

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource, TestingSink, run_main


# Step 1: Create a dataflow with a unique name
flow = Dataflow("hello_world")

# Step 2: Create an input source
# TestingSource is useful for examples and testing
# In production, you'd use KafkaSource, FileSource, etc.
input_data = ["Hello", "World", "from", "Bytewax"]
s = op.input("input", flow, TestingSource(input_data))

# Step 3: Transform the data
# The map operator applies a function to each item
s = op.map("uppercase", s, lambda word: word.upper())

# Step 4: Add another transformation
s = op.map("add_greeting", s, lambda word: f"👋 {word}!")

# Step 5: Send output to a sink
# TestingSink collects results in a list
output = []
op.output("output", s, TestingSink(output))

# Step 6: Run the dataflow
if __name__ == "__main__":
    print("Running Hello World dataflow...")
    print("-" * 50)

    run_main(flow)

    print("\nResults:")
    for item in output:
        print(f"  {item}")

    print("-" * 50)
    print(f"Processed {len(output)} items successfully!")


"""
Expected Output:
-------------------------------------------------
Results:
  👋 HELLO!
  👋 WORLD!
  👋 FROM!
  👋 BYTEWAX!
-------------------------------------------------
Processed 4 items successfully!

Key Takeaways:
1. Every dataflow needs a unique name
2. Data flows through operators using streams
3. Each operator needs a unique step_id
4. Operators are composable - chain them together
5. run_main() executes the dataflow
"""
