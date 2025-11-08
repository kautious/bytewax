"""Utilities for error handling."""

from typing import Optional


class BytewaxRuntimeError(RuntimeError):
    """A RuntimeError thrown by the Bytewax Runtime.

    When exceptions are thrown in Python from user code, errors
    can be chained from this error to provide additional context
    from the Rust runtime.
    """

    pass


class OperatorError(BytewaxRuntimeError):
    """An error that occurred in a specific operator.

    This error type includes context about which operator failed,
    making it easier to debug dataflow issues.

    Attributes:
        step_id: The fully-qualified step ID where the error occurred.
        operator_name: The name of the operator (e.g., 'map', 'filter').
        suggestion: Optional suggestion for fixing the error.
        docs_url: Optional URL to relevant documentation.

    Example:
        ```python
        try:
            # Some operator that fails
            pass
        except Exception as e:
            raise OperatorError(
                step_id="my_flow.transform",
                operator_name="map",
                message="Failed to transform item",
                suggestion="Check that your mapper function handles all input types",
            ) from e
        ```

    """

    def __init__(
        self,
        step_id: str,
        operator_name: str,
        message: str,
        suggestion: Optional[str] = None,
        docs_url: Optional[str] = None,
    ):
        """Create an OperatorError.

        Args:
            step_id: Fully-qualified step ID (e.g., 'flow.step').
            operator_name: Name of the operator (e.g., 'map').
            message: Error message.
            suggestion: Optional suggestion for fixing the error.
            docs_url: Optional URL to documentation.

        """
        self.step_id = step_id
        self.operator_name = operator_name
        self.suggestion = suggestion
        self.docs_url = docs_url

        # Build comprehensive error message
        full_message = f"[{operator_name}:{step_id}] {message}"

        if suggestion:
            full_message += f"\n💡 Suggestion: {suggestion}"

        if docs_url:
            full_message += f"\n📖 Documentation: {docs_url}"

        super().__init__(full_message)


class DataflowError(BytewaxRuntimeError):
    """An error in dataflow construction or configuration.

    This error indicates a problem with how the dataflow is defined,
    not with runtime execution.

    Example:
        ```python
        raise DataflowError(
            "Dataflow has no input operators",
            suggestion="Add an input with op.input('id', flow, source)"
        )
        ```

    """

    def __init__(self, message: str, suggestion: Optional[str] = None):
        """Create a DataflowError.

        Args:
            message: Error message.
            suggestion: Optional suggestion for fixing the error.

        """
        self.suggestion = suggestion

        full_message = message
        if suggestion:
            full_message += f"\n💡 Suggestion: {suggestion}"

        super().__init__(full_message)


class ConfigurationError(BytewaxRuntimeError):
    """An error in configuration (recovery, tracing, etc.).

    This error indicates a problem with how Bytewax is configured,
    such as invalid recovery settings or tracing configuration.

    """

    pass
