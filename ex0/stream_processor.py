from abc import ABC, abstractmethod
from typing import Any, List


class DataProcessor(ABC):

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        return isinstance(data, list) and all(isinstance(x, (int, float)) for x in data)

    def process(self, data: Any) -> str:
        if not self.validate(data):
            raise ValueError("Invalid numeric data")

        total = sum(data)
        avg = total / len(data)

        result = f"Processed {len(data)} numeric values, sum={total}, avg={avg}"
        return self.format_output(result)


class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def process(self, data: Any) -> str:
        if not self.validate(data):
            raise ValueError("Invalid text data")

        char_count = len(data)
        word_count = len(data.split())

        result = f"Processed text: {char_count} characters, {word_count} words"
        return self.format_output(result)


class LogProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def process(self, data: Any) -> str:
        if not self.validate(data):
            raise ValueError("Invalid log entry")

        if "ERROR" in data:
            level = "ALERT"
            message = data.split("ERROR:")[-1].strip()
        elif "INFO" in data:
            level = "INFO"
            message = data.split("INFO:")[-1].strip()
        else:
            level = "LOG"
            message = data

        result = f"[{level}] {message}"
        return self.format_output(result)


if __name__ == "__main__":

    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    print(numeric.process([1, 2, 3, 4, 5]))
    print(text.process("Hello Nexus World"))
    print(log.process("ERROR: Connection timeout"))

    print("\n=== Polymorphic Processing Demo ===")

    processors: List[DataProcessor] = [
        NumericProcessor(),
        TextProcessor(),
        LogProcessor()
    ]

    data_samples = [
        [1, 2, 3],
        "Hello Nexus",
        "INFO: System ready"
    ]

    for processor, data in zip(processors, data_samples):
        print(processor.process(data))
