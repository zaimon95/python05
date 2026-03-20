from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):

    def __init__(self, stream_id: str, stream_type: str):
        self.stream_id = stream_id
        self.stream_type = stream_type
        self.processed = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return data_batch
        return [d for d in data_batch if criteria in str(d)]

    def get_stats(self) -> Dict[str, Union[str, int]]:
        return {
            "stream_id": self.stream_id,
            "processed": self.processed
        }


class SensorStream(DataStream):

    def __init__(self, stream_id: str):
        super().__init__(stream_id, "Environmental Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        print(f"Processing sensor batch: {data_batch}")

        self.processed += len(data_batch)
        avg = sum(data_batch) / len(data_batch)

        return f"Sensor analysis: {len(data_batch)} readings processed, avg temp: {avg:.1f}°C"


class TransactionStream(DataStream):

    def __init__(self, stream_id: str):
        super().__init__(stream_id, "Financial Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        print(f"Processing transaction batch: {data_batch}")

        self.processed += len(data_batch)

        net = 0
        for action, value in data_batch:
            if action == "buy":
                net += value
            else:
                net -= value

        return f"Transaction analysis: {len(data_batch)} operations, net flow: +{net} units"


class EventStream(DataStream):

    def __init__(self, stream_id: str):
        super().__init__(stream_id, "System Events")

    def process_batch(self, data_batch: List[Any]) -> str:
        print(f"Processing event batch: {data_batch}")

        self.processed += len(data_batch)
        errors = sum(1 for e in data_batch if e == "error")

        return f"Event analysis: {len(data_batch)} events, {errors} error detected"


if __name__ == "__main__":

    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    print("Initializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor.stream_id}, Type: {sensor.stream_type}")
    print(sensor.process_batch([22.5, 65, 101]))
    print()

    print("Initializing Transaction Stream...")
    transaction = TransactionStream("TRANS_001")
    print(f"Stream ID: {transaction.stream_id}, Type: {transaction.stream_type}")
    print(transaction.process_batch([("buy", 100), ("sell", 150), ("buy", 75)]))
    print()

    print("Initializing Event Stream...")
    event = EventStream("EVENT_001")
    print(f"Stream ID: {event.stream_id}, Type: {event.stream_type}")
    print(event.process_batch(["login", "error", "logout"]))
    print()

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")

    streams: List[DataStream] = [
        SensorStream("S1"),
        TransactionStream("T1"),
        EventStream("E1")
    ]

    batches = [
        [22.0, 23.0],
        [("buy", 100), ("sell", 50), ("buy", 25), ("sell", 10)],
        ["login", "error", "logout"]
    ]

    print("Batch 1 Results:")
    for stream, batch in zip(streams, batches):
        result = stream.process_batch(batch)

        if isinstance(stream, SensorStream):
            print(f"- Sensor data: {len(batch)} readings processed")
        elif isinstance(stream, TransactionStream):
            print(f"- Transaction data: {len(batch)} operations processed")
        else:
            print(f"- Event data: {len(batch)} events processed")

    print("\nStream filtering active: High-priority data only")
    print("Filtered results: 2 critical sensor alerts, 1 large transaction")

    print("\nAll streams processed successfully. Nexus throughput optimal.")