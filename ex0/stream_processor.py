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
        print("Processing data:", data)

        if not self.validate(data):
            print("Validation: Invalid numeric data")
            raise ValueError("Invalid numeric data")

        print("Validation: Numeric data verified")

        total = sum(data)
        avg = total / len(data)

        result = f"Processed {len(data)} numeric values, sum={total}, avg={avg}"
        return self.format_output(result)


class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def process(self, data: Any) -> str:
        print(f'Processing data: "{data}"')

        if not self.validate(data):
            print("Validation: Invalid text data")
            raise ValueError("Invalid text data")

        print("Validation: Text data verified")

        char_count = len(data)
        word_count = len(data.split())

        result = f"Processed text: {char_count} characters, {word_count} words"
        return self.format_output(result)


class LogProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def process(self, data: Any) -> str:
        print(f'Processing data: "{data}"')

        if not self.validate(data):
            print("Validation: Invalid log entry")
            raise ValueError("Invalid log entry")

        print("Validation: Log entry verified")

        if "ERROR" in data:
            level = "ALERT"
            message = data.split("ERROR:")[-1].strip()
            result = f"[{level}] ERROR level detected: {message}"
        elif "INFO" in data:
            level = "INFO"
            message = data.split("INFO:")[-1].strip()
            result = f"[{level}] INFO level detected: {message}"
        else:
            result = f"[LOG] {data}"

        return self.format_output(result)


if __name__ == "__main__":

    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    numeric = NumericProcessor()
    print(numeric.process([1, 2, 3, 4, 5]))
    print()

    print("Initializing Text Processor...")
    text = TextProcessor()
    print(text.process("Hello Nexus World"))
    print()

    print("Initializing Log Processor...")
    log = LogProcessor()
    print(log.process("ERROR: Connection timeout"))
    print()

    print("=== Polymorphic Processing Demo ===\n")

    print("Processing multiple data types through same interface...")

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

    for i, (processor, data) in enumerate(zip(processors, data_samples), start=1):
        result = processor.process(data)
        print(f"Result {i}: {result.replace('Output: ', '')}")
