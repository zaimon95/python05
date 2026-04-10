from abc import ABC, abstractmethod
from typing import Any, Union
import typing


class DataProcessor(ABC):
    """Abstract base class defining the common processing interface."""

    def __init__(self) -> None:
        self._storage: list[str] = []
        self._total_processed: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Check whether input data is appropriate for this processor."""
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        """Process and store input data."""
        ...

    def output(self) -> tuple[int, str]:
        """Extract and remove the oldest stored item with its processing rank."""
        if not self._storage:
            raise IndexError("No data available in processor")
        rank = self._total_processed - len(self._storage)
        value = self._storage.pop(0)
        return rank, value

    @property
    def storage(self):
        return self._storage

    @property
    def total_processed(self):
        return self._total_processed


# ---------------------------------------------------------------------------
# Specialized processors
# ---------------------------------------------------------------------------

NumericInput = Union[int, float, list[Union[int, float]]]


class NumericProcessor(DataProcessor):
    """Processes int, float, and mixed lists of both."""

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            return True
        if isinstance(data, list):
            return all(
                isinstance(item, (int, float)) and not isinstance(item, bool)
                for item in data
            )
        return False

    def ingest(self, data: NumericInput) -> None:
        if not self.validate(data):
            raise TypeError("Improper numeric data")
        if isinstance(data, list):
            for item in data:
                self._storage.append(str(item))
                self._total_processed += 1
        else:
            self._storage.append(str(data))
            self._total_processed += 1


TextInput = Union[str, list[str]]


class TextProcessor(DataProcessor):
    """Processes str and lists of strings."""

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)
        return False

    def ingest(self, data: TextInput) -> None:
        if not self.validate(data):
            raise TypeError("Improper text data")
        if isinstance(data, list):
            for item in data:
                self._storage.append(item)
                self._total_processed += 1
        else:
            self._storage.append(data)
            self._total_processed += 1


LogEntry = dict[str, str]
LogInput = Union[LogEntry, list[LogEntry]]


class LogProcessor(DataProcessor):
    """Processes dicts of string key-value pairs and lists of those."""

    def validate(self, data: Any) -> bool:
        def is_log_entry(item: Any) -> bool:
            return (
                isinstance(item, dict)
                and all(isinstance(k, str) and isinstance(v, str)
                        for k, v in item.items())
            )
        if is_log_entry(data):
            return True
        if isinstance(data, list):
            return all(is_log_entry(item) for item in data)
        return False

    def ingest(self, data: LogInput) -> None:
        if not self.validate(data):
            raise TypeError("Improper log data")
        entries = data if isinstance(data, list) else [data]
        for entry in entries:
            level = entry.get("log_level", "")
            message = entry.get("log_message", "")
            self._storage.append(f"{level}: {message}")
            self._total_processed += 1


# ---------------------------------------------------------------------------
# DataStream — routes elements to the right processor
# ---------------------------------------------------------------------------

class DataStream:
    """Routes incoming data elements to registered processors."""

    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        """Register a new processor to the stream."""
        self._processors.append(proc)

    def process_stream(self, stream_processed: list[typing.Any]) -> None:
        """Route each element to the first compatible processor."""
        for element in stream_processed:
            handled = False
            for proc in self._processors:
                if proc.validate(element):
                    proc.ingest(element)
                    handled = True
                    break
            if not handled:
                print(
                    f"DataStream error - Can't process element in stream: "
                    f"{element}"
                )

    def print_processors_stats(self) -> None:
        """Print current statistics for all registered processors."""
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            name = type(proc).__name__
            remaining = len(proc.storage)
            total = proc.total_processed
            print(
                f"{name}: total {total} items processed, "
                f"remaining {remaining} on processor"
            )


# ---------------------------------------------------------------------------
# Main — tests
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")

    print("\nInitialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Numeric Processor")
    num_proc = NumericProcessor()
    stream.register_processor(num_proc)

    batch: list[Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {"log_level": "WARNING", "log_message": "Telnet access! Use ssh instead"},
            {"log_level": "INFO", "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]

    print(f"Send first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nRegistering other data processors")
    txt_proc = TextProcessor()
    log_proc = LogProcessor()
    stream.register_processor(txt_proc)
    stream.register_processor(log_proc)

    print("Send the same batch again")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nConsume some elements from the data processors: Numeric 3, Text 2, Log 1")
    for _ in range(3):
        num_proc.output()
    for _ in range(2):
        txt_proc.output()
    log_proc.output()
    stream.print_processors_stats()
