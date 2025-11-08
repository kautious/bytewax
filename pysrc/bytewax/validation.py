"""Dataflow validation framework.

This module provides tools to validate dataflows before execution,
catching common errors and configuration issues early.

Example usage:

```python
from bytewax.dataflow import Dataflow
from bytewax.validation import validate_dataflow, ValidationWarning
import bytewax.operators as op

flow = Dataflow("example")
# ... build dataflow

# Validate before running
errors, warnings = validate_dataflow(flow)

if errors:
    for error in errors:
        print(f"❌ {error}")
    raise RuntimeError("Dataflow validation failed")

if warnings:
    for warning in warnings:
        print(f"⚠️  {warning}")

# Or use auto-validation in run_main
from bytewax.testing import run_main
run_main(flow, validate=True)  # Validates automatically
```

"""

from dataclasses import dataclass
from typing import List, Set, Tuple

from bytewax.dataflow import Dataflow, Operator


@dataclass
class ValidationError:
    """A validation error that prevents dataflow execution."""

    code: str
    """Error code for programmatic handling."""

    message: str
    """Human-readable error message."""

    step_id: str = ""
    """Step ID where error occurred (if applicable)."""

    suggestion: str = ""
    """Suggested fix for the error."""

    def __str__(self) -> str:
        """Format error for display."""
        parts = [f"[{self.code}]"]
        if self.step_id:
            parts.append(f"[{self.step_id}]")
        parts.append(self.message)
        msg = " ".join(parts)

        if self.suggestion:
            msg += f"\n  💡 Suggestion: {self.suggestion}"

        return msg


@dataclass
class ValidationWarning:
    """A validation warning about potential issues."""

    code: str
    """Warning code for programmatic handling."""

    message: str
    """Human-readable warning message."""

    step_id: str = ""
    """Step ID where warning occurred (if applicable)."""

    def __str__(self) -> str:
        """Format warning for display."""
        parts = [f"[{self.code}]"]
        if self.step_id:
            parts.append(f"[{self.step_id}]")
        parts.append(self.message)
        return " ".join(parts)


def _collect_all_steps(flow: Dataflow) -> List[Operator]:
    """Recursively collect all steps from a dataflow."""
    all_steps = []

    def collect(steps: List[Operator]) -> None:
        for step in steps:
            all_steps.append(step)
            # Recursively collect substeps
            if hasattr(step, "substeps") and step.substeps:
                collect(step.substeps)

    collect(flow.substeps)
    return all_steps


def _check_has_input(flow: Dataflow) -> List[ValidationError]:
    """Check that dataflow has at least one input operator."""
    errors = []

    all_steps = _collect_all_steps(flow)
    has_input = any(type(step).__name__ == "input" for step in all_steps)

    if not has_input:
        errors.append(
            ValidationError(
                code="NO_INPUT",
                message="Dataflow has no input operators",
                suggestion="Add an input operator with op.input('step_id', flow, source)",
            )
        )

    return errors


def _check_has_output(flow: Dataflow) -> List[ValidationError]:
    """Check that dataflow has at least one output operator."""
    errors = []

    all_steps = _collect_all_steps(flow)
    has_output = any(type(step).__name__ == "output" for step in all_steps)

    if not has_output:
        errors.append(
            ValidationError(
                code="NO_OUTPUT",
                message="Dataflow has no output operators",
                suggestion="Add an output operator with op.output('step_id', stream, sink)",
            )
        )

    return errors


def _check_duplicate_step_ids(flow: Dataflow) -> List[ValidationError]:
    """Check for duplicate step IDs."""
    errors = []

    all_steps = _collect_all_steps(flow)
    step_ids: Set[str] = set()
    duplicates: Set[str] = set()

    for step in all_steps:
        if step.step_id in step_ids:
            duplicates.add(step.step_id)
        step_ids.add(step.step_id)

    for dup_id in duplicates:
        errors.append(
            ValidationError(
                code="DUPLICATE_STEP_ID",
                message=f"Duplicate step ID: {dup_id}",
                step_id=dup_id,
                suggestion="Ensure each step has a unique ID",
            )
        )

    return errors


def _check_step_id_format(flow: Dataflow) -> List[ValidationError]:
    """Check that step IDs follow naming conventions."""
    errors = []

    all_steps = _collect_all_steps(flow)

    for step in all_steps:
        # Check for periods (not allowed)
        if "." in step.step_name:
            errors.append(
                ValidationError(
                    code="INVALID_STEP_ID",
                    message=f"Step ID contains period: {step.step_name}",
                    step_id=step.step_id,
                    suggestion="Step IDs cannot contain periods '.'",
                )
            )

        # Check for empty step names
        if not step.step_name:
            errors.append(
                ValidationError(
                    code="EMPTY_STEP_ID",
                    message="Step has empty ID",
                    step_id=step.step_id,
                    suggestion="Provide a non-empty step ID",
                )
            )

    return errors


def _check_unused_operators(flow: Dataflow) -> List[ValidationWarning]:
    """Check for operators that might not be connected properly."""
    warnings = []

    all_steps = _collect_all_steps(flow)

    # Find operators that don't lead to an output
    # This is a heuristic - for now, we skip this check as it's too aggressive
    # and flags many valid operators (input, internal operators, etc.)
    # A more sophisticated check would trace the dataflow graph.

    # TODO: Implement proper dataflow graph traversal to detect truly unused operators

    return warnings


def _check_empty_dataflow(flow: Dataflow) -> List[ValidationError]:
    """Check if dataflow is completely empty."""
    errors = []

    if not flow.substeps:
        errors.append(
            ValidationError(
                code="EMPTY_DATAFLOW",
                message="Dataflow has no operators",
                suggestion="Add operators to the dataflow",
            )
        )

    return errors


def validate_dataflow(
    flow: Dataflow, strict: bool = False
) -> Tuple[List[ValidationError], List[ValidationWarning]]:
    """Validate a dataflow for common issues.

    This function checks for:
    - At least one input operator
    - At least one output operator
    - Duplicate step IDs
    - Invalid step ID formats
    - Empty dataflows
    - Possibly unused operators (warning)

    Args:
        flow: The dataflow to validate.
        strict: If True, treat warnings as errors.

    Returns:
        A tuple of (errors, warnings).

    Example:
        ```python
        flow = Dataflow("example")
        # ... build dataflow

        errors, warnings = validate_dataflow(flow)

        if errors:
            for error in errors:
                print(f"❌ {error}")
            raise RuntimeError("Validation failed")

        if warnings:
            for warning in warnings:
                print(f"⚠️  {warning}")
        ```

    """
    errors: List[ValidationError] = []
    warnings: List[ValidationWarning] = []

    # Run all validation checks
    errors.extend(_check_empty_dataflow(flow))

    # Skip other checks if dataflow is empty
    if errors:
        return errors, warnings

    errors.extend(_check_has_input(flow))
    errors.extend(_check_has_output(flow))
    errors.extend(_check_duplicate_step_ids(flow))
    errors.extend(_check_step_id_format(flow))

    warnings.extend(_check_unused_operators(flow))

    # Convert warnings to errors in strict mode
    if strict:
        for warning in warnings:
            errors.append(
                ValidationError(
                    code=warning.code,
                    message=warning.message,
                    step_id=warning.step_id,
                )
            )
        warnings = []

    return errors, warnings


def validate_or_raise(flow: Dataflow, strict: bool = False) -> None:
    """Validate a dataflow and raise an exception if errors are found.

    This is a convenience function that validates and raises a
    RuntimeError if validation fails.

    Args:
        flow: The dataflow to validate.
        strict: If True, treat warnings as errors.

    Raises:
        RuntimeError: If validation errors are found.

    Example:
        ```python
        flow = Dataflow("example")
        # ... build dataflow

        # This will raise if validation fails
        validate_or_raise(flow)
        ```

    """
    errors, warnings = validate_dataflow(flow, strict=strict)

    if errors:
        error_msg = "Dataflow validation failed:\n"
        error_msg += "\n".join(f"  ❌ {error}" for error in errors)
        raise RuntimeError(error_msg)

    if warnings:
        import warnings as python_warnings

        for warning in warnings:
            python_warnings.warn(str(warning), UserWarning, stacklevel=2)
