from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):

    def __init__(self, stream_id: str):
        self.stream_id = stream_id
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

    def process_batch(self, data_batch: List[Any]) -> str:
        self.processed += len(data_batch)
        avg = sum(data_batch) / len(data_batch)
        return f"Sensor analysis: {len(data_batch)} readings processed, avg: {avg}"


class TransactionStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        self.processed += len(data_batch)

        net = 0
        for action, value in data_batch:
            if action == "buy":
                net += value
            else:
                net -= value

        return f"Transaction analysis: {len(data_batch)} operations, net flow: {net}"


class EventStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        self.processed += len(data_batch)

        errors = sum(1 for e in data_batch if e == "error")

        return f"Event analysis: {len(data_batch)} events, {errors} error detected"


class StreamProcessor:

    def __init__(self):
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        self.streams.append(stream)

    def process_all(self, batches: List[List[Any]]) -> None:

        for stream, batch in zip(self.streams, batches):
            print(stream.process_batch(batch))
            print("Stats:", stream.get_stats())


if __name__ == "__main__":

    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    sensor = SensorStream("SENSOR_001")
    transaction = TransactionStream("TRANS_001")
    event = EventStream("EVENT_001")

    processor = StreamProcessor()

    processor.add_stream(sensor)
    processor.add_stream(transaction)
    processor.add_stream(event)

    batches = [
        [22.5, 23.0, 21.8],
        [("buy", 100), ("sell", 50), ("buy", 75)],
        ["login", "error", "logout"]
    ]

    processor.process_all(batches)
