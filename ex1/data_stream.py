from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    """Abstract base class with core streaming functionality."""

    def __init__(self, stream_id: str) -> None:
        self.stream_id: str = stream_id
        self._processed_count: int = 0
        self._error_count: int = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data and return a summary string."""
        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Default filter: return all items (subclasses may override)."""
        if criteria is None:
            return data_batch
        return [item for item in data_batch if criteria in str(item)]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return basic stream statistics."""
        return {
            "stream_id": self.stream_id,
            "processed": self._processed_count,
            "errors": self._error_count,
        }


class SensorStream(DataStream):
    """Stream handler specialised for environmental sensor readings."""

    STREAM_TYPE: str = "Environmental Data"

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self._temperature_sum: float = 0.0
        self._temperature_count: int = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Parse sensor readings and compute average temperature."""
        try:
            temps: List[float] = []
            for item in data_batch:
                item_str: str = str(item)
                if "temp" in item_str.lower():
                    # Format expected: "temp:22.5"
                    value_part: str = item_str.split(":", 1)[1]
                    temps.append(float(value_part))
            self._processed_count += len(data_batch)
            if temps:
                avg_temp: float = sum(temps) / len(temps)
                self._temperature_sum += sum(temps)
                self._temperature_count += len(temps)
                return (
                    f"Sensor analysis: {len(data_batch)} readings processed, "
                    f"avg temp: {avg_temp}°C"
                )
            return f"Sensor analysis: {len(data_batch)} readings processed"
        except (ValueError, IndexError) as e:
            self._error_count += 1
            return f"Sensor processing error: {e}"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter: keep only items flagged as 'alert' when criteria='alert'."""
        if criteria == "alert":
            return [
                item for item in data_batch
                if "alert" in str(item).lower()
            ]
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats: Dict[str, Union[str, int, float]] = super().get_stats()
        stats["stream_type"] = self.STREAM_TYPE
        if self._temperature_count > 0:
            stats["avg_temp"] = round(
                self._temperature_sum / self._temperature_count, 2
            )
        return stats


class TransactionStream(DataStream):
    """Stream handler specialised for financial transaction data."""

    STREAM_TYPE: str = "Financial Data"

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self._net_flow: float = 0.0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Parse buy/sell transactions and compute net flow."""
        try:
            net: float = 0.0
            ops: int = 0
            for item in data_batch:
                item_str: str = str(item)
                if ":" in item_str:
                    op, val_str = item_str.split(":", 1)
                    value: float = float(val_str)
                    if op.strip().lower() == "buy":
                        net += value
                    elif op.strip().lower() == "sell":
                        net -= value
                    ops += 1
            self._processed_count += ops
            self._net_flow += net
            sign: str = "+" if net >= 0 else ""
            return (
                f"Transaction analysis: {ops} operations, "
                f"net flow: {sign}{net:.0f} units"
            )
        except (ValueError, IndexError) as e:
            self._error_count += 1
            return f"Transaction processing error: {e}"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter: keep transactions above threshold when criteria='large'."""
        if criteria == "large":
            result: List[Any] = []
            for item in data_batch:
                item_str: str = str(item)
                if ":" in item_str:
                    try:
                        value: float = float(item_str.split(":", 1)[1])
                        if value >= 100:
                            result.append(item)
                    except ValueError:
                        pass
            return result
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats: Dict[str, Union[str, int, float]] = super().get_stats()
        stats["stream_type"] = self.STREAM_TYPE
        stats["net_flow"] = self._net_flow
        return stats


class EventStream(DataStream):
    """Stream handler specialised for system event data."""

    STREAM_TYPE: str = "System Events"
    ERROR_KEYWORDS: List[str] = ["error", "fail", "critical"]

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self._event_errors: int = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Scan events for errors and report counts."""
        try:
            errors: int = sum(
                1 for item in data_batch
                if any(kw in str(item).lower() for kw in self.ERROR_KEYWORDS)
            )
            self._processed_count += len(data_batch)
            self._event_errors += errors
            return (
                f"Event analysis: {len(data_batch)} events, "
                f"{errors} error{'s' if errors != 1 else ''} detected"
            )
        except Exception as e:
            self._error_count += 1
            return f"Event processing error: {e}"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter: keep only error events when criteria='error'."""
        if criteria == "error":
            return [
                item for item in data_batch
                if any(kw in str(item).lower() for kw in self.ERROR_KEYWORDS)
            ]
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats: Dict[str, Union[str, int, float]] = super().get_stats()
        stats["stream_type"] = self.STREAM_TYPE
        stats["event_errors"] = self._event_errors
        return stats


class StreamProcessor:
    """Manages and processes multiple DataStream instances polymorphically."""

    def __init__(self) -> None:
        self._streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        """Register a stream for processing."""
        self._streams.append(stream)

    def process_all(
        self,
        batches: List[List[Any]]
    ) -> List[str]:
        """Process each registered stream with its corresponding batch."""
        results: List[str] = []
        for stream, batch in zip(self._streams, batches):
            try:
                results.append(stream.process_batch(batch))
            except Exception as e:
                results.append(f"Stream {stream.stream_id} error: {e}")
        return results

    def filter_all(
        self,
        batches: List[List[Any]],
        criteria: Optional[str] = None
    ) -> List[List[Any]]:
        """Apply each stream's filter to its corresponding batch."""
        return [
            stream.filter_data(batch, criteria)
            for stream, batch in zip(self._streams, batches)
        ]


def main() -> None:
    """Entry point: demonstrate polymorphic stream processing."""
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()

    sensor: SensorStream = SensorStream("SENSOR_001")
    print("Initializing Sensor Stream...")
    print(f"Stream ID: {sensor.stream_id}, Type: {SensorStream.STREAM_TYPE}")
    sensor_batch: List[Any] = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(f"Processing sensor batch: {sensor_batch}")
    print(sensor.process_batch(sensor_batch))
    print()

    trans: TransactionStream = TransactionStream("TRANS_001")
    print("Initializing Transaction Stream...")
    print(
        f"Stream ID: {trans.stream_id}, "
        f"Type: {TransactionStream.STREAM_TYPE}"
    )
    trans_batch: List[Any] = ["buy:100", "sell:150", "buy:75"]
    print(f"Processing transaction batch: {trans_batch}")
    print(trans.process_batch(trans_batch))
    print()

    event: EventStream = EventStream("EVENT_001")
    print("Initializing Event Stream...")
    print(
        f"Stream ID: {event.stream_id}, "
        f"Type: {EventStream.STREAM_TYPE}"
    )
    event_batch: List[Any] = ["login", "error", "logout"]
    print(f"Processing event batch: {event_batch}")
    print(event.process_batch(event_batch))
    print()

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print()

    processor: StreamProcessor = StreamProcessor()
    s2: SensorStream = SensorStream("SENSOR_002")
    t2: TransactionStream = TransactionStream("TRANS_002")
    e2: EventStream = EventStream("EVENT_002")
    processor.add_stream(s2)
    processor.add_stream(t2)
    processor.add_stream(e2)

    mixed_batches: List[List[Any]] = [
        ["temp:20.0", "temp:21.0"],
        ["buy:200", "sell:50", "buy:300", "sell:100"],
        ["login", "logout", "error"],
    ]
    results: List[str] = processor.process_all(mixed_batches)

    print("Batch 1 Results:")
    labels: List[str] = ["Sensor data", "Transaction data", "Event data"]
    for label, res in zip(labels, results):
        # Extract count from result string for the short summary
        count_part: str = res.split(":")[1].strip() if ":" in res else res
        print(f"- {label}: {count_part}")
    print()

    print("Stream filtering active: High-priority data only")
    filter_batches: List[List[Any]] = [
        [
            "temp:25.0", "alert:critical_heat",
            "temp:18.0", "alert:pressure_drop",
        ],
        ["buy:50", "sell:200", "buy:150"],
        ["login", "error", "logout"],
    ]
    filtered: List[List[Any]] = [
        s2.filter_data(filter_batches[0], "alert"),
        t2.filter_data(filter_batches[1], "large"),
        e2.filter_data(filter_batches[2], "error"),
    ]
    sensor_alerts: int = len(filtered[0])
    large_txns: int = len(filtered[1])
    print(
        f"Filtered results: {sensor_alerts} critical sensor alerts, "
        f"{large_txns} large transaction{'s' if large_txns != 1 else ''}"
    )
    print()
    print("All streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
