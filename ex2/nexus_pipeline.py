from abc import ABC, abstractmethod
from typing import Any, Union, Protocol
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
        return (rank, value)


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
# Export plugin protocol (duck typing)
# ---------------------------------------------------------------------------

class ExportPlugin(Protocol):
    """Protocol that all export plugins must satisfy."""

    def process_output(self, data: list[tuple[int, str]]) -> None:
        """Export a list of (rank, value) tuples."""
        ...


class CSVExportPlugin:
    """Exports data as a single CSV row per call."""

    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        row = ",".join(value for _, value in data)
        print("CSV Output:")
        print(row)


class JSONExportPlugin:
    """Exports data as a JSON object with item_<rank> keys."""

    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        pairs = ", ".join(
            f'"item_{rank}": "{value}"' for rank, value in data
        )
        print("JSON Output:")
        print("{" + pairs + "}")


# ---------------------------------------------------------------------------
# DataStream — routes elements and exports via plugins
# ---------------------------------------------------------------------------

class DataStream:
    """Routes incoming data elements to registered processors."""

    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        """Register a new processor to the stream."""
        self._processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        """Route each element to the first compatible processor."""
        for element in stream:
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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        """Consume nb elements from each processor and export via plugin."""
        for proc in self._processors:
            collected: list[tuple[int, str]] = []
            for _ in range(nb):
                if not proc._storage:
                    break
                collected.append(proc.output())
            plugin.process_output(collected)

    def print_processors_stats(self) -> None:
        """Print current statistics for all registered processors."""
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            name = type(proc).__name__
            remaining = len(proc._storage)
            total = proc._total_processed
            print(
                f"{name}: total {total} items processed, "
                f"remaining {remaining} on processor"
            )


# ---------------------------------------------------------------------------
# Main — tests
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===")

    print("\nInitialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Processors")
    stream.register_processor(NumericProcessor())
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())

    batch1: list[Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {"log_level": "WARNING", "log_message": "Telnet access! Use ssh instead"},
            {"log_level": "INFO", "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]

    print(f"Send first batch of data on stream: {batch1}")
    stream.process_stream(batch1)
    stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVExportPlugin()
    stream.output_pipeline(3, csv_plugin)
    stream.print_processors_stats()

    batch2: list[Any] = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {"log_level": "ERROR", "log_message": "500 server crash"},
            {"log_level": "NOTICE", "log_message": "Certificate expires in 10 days"},
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]

    print(f"\nSend another batch of data: {batch2}")
    stream.process_stream(batch2)
    stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONExportPlugin()
    stream.output_pipeline(5, json_plugin)
    stream.print_processors_stats()
