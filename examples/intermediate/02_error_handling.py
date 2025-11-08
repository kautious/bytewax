"""Error Handling - Handling Failures Gracefully

This example demonstrates error handling patterns in dataflows:
- Try-except in mapper functions
- Filtering out errors
- Collecting errors separately
- Recovery strategies

Concepts introduced:
- Error handling in operators
- Separate error streams
- Error logging and recovery
- Fault tolerance patterns
"""

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource, TestingSink, run_main


def example_1_basic_error_handling():
    """Example 1: Basic try-except in mapper."""
    print("\n" + "=" * 60)
    print("Example 1: Basic Error Handling")
    print("=" * 60)

    flow = Dataflow("basic_error_handling")

    # Data with some invalid items
    data = ["10", "20", "invalid", "30", "bad", "40"]
    s = op.input("data", flow, TestingSource(data))

    # Try to convert to int, use None for errors
    def safe_int(x):
        try:
            return int(x)
        except ValueError:
            return None

    s = op.map("to_int", s, safe_int)

    # Filter out None values
    s = op.filter("valid_only", s, lambda x: x is not None)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print(f"\nInput:  {data}")
    print(f"Output: {output}")
    print(f"Valid items: {len(output)}/{len(data)}")


def example_2_separate_error_stream():
    """Example 2: Split into success and error streams."""
    print("\n" + "=" * 60)
    print("Example 2: Separate Error Stream")
    print("=" * 60)

    flow = Dataflow("separate_errors")

    data = ["10", "20", "invalid", "30", "bad", "40"]
    s = op.input("data", flow, TestingSource(data))

    # Convert to Either success or error
    def try_parse(x):
        try:
            return ("success", int(x))
        except ValueError:
            return ("error", x)

    s = op.map("try_parse", s, try_parse)

    # Branch into success and error streams
    branches = op.branch("split", s, lambda x: x[0] == "success")

    # Success stream
    success = branches.trues
    success = op.map("extract_value", success, lambda x: x[1])

    # Error stream
    errors = branches.falses
    errors = op.map("extract_error", errors, lambda x: x[1])

    success_out = []
    error_out = []
    op.output("success", success, TestingSink(success_out))
    op.output("errors", errors, TestingSink(error_out))

    run_main(flow)

    print(f"\nInput:   {data}")
    print(f"Success: {success_out}")
    print(f"Errors:  {error_out}")


def example_3_error_logging():
    """Example 3: Log errors while processing."""
    print("\n" + "=" * 60)
    print("Example 3: Error Logging")
    print("=" * 60)

    flow = Dataflow("error_logging")

    data = ["10", "20", "bad1", "30", "bad2", "40"]
    s = op.input("data", flow, TestingSource(data))

    error_log = []

    def parse_with_logging(x):
        try:
            return int(x)
        except ValueError as e:
            error_log.append({"value": x, "error": str(e)})
            return None

    s = op.map("parse", s, parse_with_logging)
    s = op.filter("valid", s, lambda x: x is not None)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print(f"\nSuccessfully parsed: {output}")
    print("\nError log:")
    for log in error_log:
        print(f"  Value '{log['value']}': {log['error']}")


def example_4_default_values():
    """Example 4: Use default values for errors."""
    print("\n" + "=" * 60)
    print("Example 4: Default Values on Error")
    print("=" * 60)

    flow = Dataflow("default_values")

    data = ["10", "20", "invalid", "30", "bad", "40"]
    s = op.input("data", flow, TestingSource(data))

    # Use 0 as default for parse errors
    def parse_or_default(x, default=0):
        try:
            return int(x)
        except ValueError:
            return default

    s = op.map("parse", s, parse_or_default)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print(f"\nInput:  {data}")
    print(f"Output: {output}")
    print("Note: Invalid values replaced with 0")


def example_5_error_recovery():
    """Example 5: Multiple recovery strategies."""
    print("\n" + "=" * 60)
    print("Example 5: Error Recovery Strategies")
    print("=" * 60)

    flow = Dataflow("error_recovery")

    # JSON-like data with some invalid entries
    data = [
        '{"value": 10}',
        '{"value": 20}',
        'invalid json',
        '{"value": "not_a_number"}',
        '{"value": 30}',
    ]

    s = op.input("data", flow, TestingSource(data))

    def safe_parse(json_str):
        import json

        try:
            # Try to parse JSON
            obj = json.loads(json_str)

            # Try to extract value
            value = obj.get("value")

            # Try to convert to int
            return int(value)

        except json.JSONDecodeError:
            print(f"  ⚠️  JSON parse error: {json_str[:30]}...")
            return None
        except (TypeError, ValueError):
            print(f"  ⚠️  Value conversion error: {json_str[:30]}...")
            return None
        except Exception as e:
            print(f"  ⚠️  Unexpected error: {e}")
            return None

    s = op.map("safe_parse", s, safe_parse)
    s = op.filter("valid", s, lambda x: x is not None)

    output = []
    op.output("out", s, TestingSink(output))

    print("\nProcessing:")
    run_main(flow)

    print(f"\nSuccessfully processed: {output}")


def example_6_validation():
    """Example 6: Data validation pipeline."""
    print("\n" + "=" * 60)
    print("Example 6: Data Validation Pipeline")
    print("=" * 60)

    flow = Dataflow("validation")

    # User data with validation issues
    users = [
        {"name": "Alice", "age": 30, "email": "alice@example.com"},
        {"name": "Bob", "age": -5, "email": "bob@example.com"},  # Invalid age
        {"name": "Charlie", "age": 25, "email": "invalid-email"},  # Invalid email
        {"name": "", "age": 35, "email": "dave@example.com"},  # Missing name
        {"name": "Eve", "age": 28, "email": "eve@example.com"},
    ]

    s = op.input("users", flow, TestingSource(users))

    def validate_user(user):
        errors = []

        # Validate name
        if not user.get("name"):
            errors.append("Missing name")

        # Validate age
        age = user.get("age", 0)
        if age <= 0 or age > 150:
            errors.append(f"Invalid age: {age}")

        # Validate email
        email = user.get("email", "")
        if "@" not in email:
            errors.append(f"Invalid email: {email}")

        if errors:
            return ("invalid", {"user": user, "errors": errors})
        else:
            return ("valid", user)

    s = op.map("validate", s, validate_user)

    # Split into valid and invalid
    branches = op.branch("split", s, lambda x: x[0] == "valid")

    valid = branches.trues
    valid = op.map("extract", valid, lambda x: x[1])

    invalid = branches.falses
    invalid = op.map("extract_invalid", invalid, lambda x: x[1])

    valid_out = []
    invalid_out = []
    op.output("valid", valid, TestingSink(valid_out))
    op.output("invalid", invalid, TestingSink(invalid_out))

    run_main(flow)

    print(f"\nValid users ({len(valid_out)}):")
    for user in valid_out:
        print(f"  {user['name']}, {user['age']}, {user['email']}")

    print(f"\nInvalid users ({len(invalid_out)}):")
    for item in invalid_out:
        user = item["user"]
        print(f"  {user.get('name', 'unnamed')}: {', '.join(item['errors'])}")


if __name__ == "__main__":
    print("\n⚠️  Error Handling Examples")
    print("Learn to handle failures gracefully")

    example_1_basic_error_handling()
    example_2_separate_error_stream()
    example_3_error_logging()
    example_4_default_values()
    example_5_error_recovery()
    example_6_validation()

    print("\n" + "=" * 60)
    print("✅ All error handling examples completed!")
    print("=" * 60)


"""
Key Takeaways:

1. TRY-EXCEPT in mappers:
   - Wrap risky operations in try-except
   - Return None, default, or error marker
   - Let data continue flowing

2. SEPARATE ERROR STREAMS:
   - Use branch to split success/error
   - Process each stream differently
   - Output to different sinks

3. ERROR PATTERNS:
   a) Filter out errors (drop them)
   b) Replace with defaults
   c) Log and continue
   d) Separate error stream

4. VALIDATION:
   - Validate early in pipeline
   - Collect all validation errors
   - Provide clear error messages

5. RECOVERY STRATEGIES:
   - Use defaults for missing data
   - Log errors for debugging
   - Retry with backoff (for I/O)
   - Dead letter queue (error output)

6. BEST PRACTICES:
   - Fail gracefully, don't crash
   - Log enough context for debugging
   - Monitor error rates
   - Have fallback strategies

Next steps:
- See Phase 2 features for enhanced error messages
- Use validation framework for dataflow validation
- Consider using OperatorError for better context
"""
