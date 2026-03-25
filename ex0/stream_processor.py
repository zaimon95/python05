from abc import ABC, abstractmethod
from typing import Any, List, Union, Optional


class DataProcessor(ABC):
    """Abstract base class defining the common processing interface."""

    @abstractmethod
    def process(self, data: Any) -> str:
        """Process the data and return result string."""
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate if data is appropriate for this processor."""
        pass

    def format_output(self, result: str) -> str:
        """Format the output string (default implementation)."""
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    """Processor specialized for numeric data (lists of numbers)."""

    def validate(self, data: Any) -> bool:
        """Return True if data is a non-empty list of numbers."""
        if not isinstance(data, list) or len(data) == 0:
            return False
        return all(isinstance(x, (int, float)) for x in data)

    def process(self, data: Any) -> str:
        """Process numeric list: compute sum and average."""
        if not self.validate(data):
            raise ValueError("Invalid numeric data")
        total: Union[int, float] = sum(data)
        avg: float = total / len(data)
        return (
            f"Processed {len(data)} numeric values, "
            f"sum={total}, avg={avg}"
        )

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class TextProcessor(DataProcessor):
    """Processor specialized for text (string) data."""

    def validate(self, data: Any) -> bool:
        """Return True if data is a non-empty string."""
        return isinstance(data, str) and len(data) > 0

    def process(self, data: Any) -> str:
        """Process text: count characters and words."""
        if not self.validate(data):
            raise ValueError("Invalid text data")
        chars: int = len(data)
        words: int = len(data.split())
        return f"Processed text: {chars} characters, {words} words"

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class LogProcessor(DataProcessor):
    """Processor specialized for log entry strings."""

    LOG_LEVELS: List[str] = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

    def validate(self, data: Any) -> bool:
        """Return True if data is a string containing a known log level."""
        if not isinstance(data, str):
            return False
        return any(level in data.upper() for level in self.LOG_LEVELS)

    def process(self, data: Any) -> str:
        """Process a log entry: detect level and extract message."""
        if not self.validate(data):
            raise ValueError("Invalid log entry")
        detected_level: str = "UNKNOWN"
        for level in self.LOG_LEVELS:
            if level in data.upper():
                detected_level = level
                break
        # Extract message after the first colon if present
        parts: List[str] = data.split(":", 1)
        message: str = parts[1].strip() if len(parts) > 1 else data
        is_critical: bool = detected_level in ("ERROR", "CRITICAL")
        alert: str = "ALERT" if is_critical else detected_level
        return f"[{alert}] {detected_level} level detected: {message}"

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


def demo_processor(
    processor: DataProcessor,
    label: str,
    data: Any,
    validation_msg: str
) -> Optional[str]:
    """Run a single processor demo and print results."""
    print(f"Initializing {label}...")
    print(f"Processing data: {repr(data)}")
    if processor.validate(data):
        print(f"Validation: {validation_msg}")
        try:
            result: str = processor.process(data)
            print(processor.format_output(result))
            return result
        except ValueError as e:
            print(f"Processing error: {e}")
    else:
        print("Validation: FAILED")
    return None


def main() -> None:
    """Entry point: demonstrate polymorphic data processing."""
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()

    numeric: NumericProcessor = NumericProcessor()
    demo_processor(
        numeric, "Numeric Processor", [1, 2, 3, 4, 5],
        "Numeric data verified"
    )
    print()

    text: TextProcessor = TextProcessor()
    demo_processor(
        text, "Text Processor", "Hello Nexus World",
        "Text data verified"
    )
    print()

    log: LogProcessor = LogProcessor()
    demo_processor(
        log, "Log Processor", "ERROR: Connection timeout",
        "Log entry verified"
    )
    print()

    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    processors: List[DataProcessor] = [
        NumericProcessor(),
        TextProcessor(),
        LogProcessor(),
    ]
    datasets: List[Any] = [
        [1, 2, 3],
        "Hello World",
        "INFO: System ready",
    ]

    for idx, (proc, data) in enumerate(zip(processors, datasets), start=1):
        try:
            result: str = proc.process(data)
            print(f"Result {idx}: {result}")
        except ValueError as e:
            print(f"Result {idx}: Error - {e}")

    print()
    print("Foundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
