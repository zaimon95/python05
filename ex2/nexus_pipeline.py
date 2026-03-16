from abc import ABC, abstractmethod
from typing import Any, List, Protocol, Union, Dict
import json


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


class InputStage:

    def process(self, data: Any) -> Any:
        return data


class TransformStage:

    def process(self, data: Any) -> Any:
        if isinstance(data, dict):
            data["processed"] = True
        return data


class OutputStage:

    def process(self, data: Any) -> Any:
        return f"Output: {data}"


class ProcessingPipeline(ABC):

    def __init__(self):
        self.stages: List[ProcessingStage] = [
            InputStage(),
            TransformStage(),
            OutputStage()
        ]

    @abstractmethod
    def process(self, data: Any) -> Any:
        pass

    def run_stages(self, data: Any) -> Any:
        for stage in self.stages:
            data = stage.process(data)
        return data


class JSONAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:

        parsed = json.loads(data)

        result = self.run_stages(parsed)

        return result


class CSVAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:

        fields = data.split(",")

        result = self.run_stages(fields)

        return result


class StreamAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:

        result = self.run_stages(data)

        return result


class NexusManager:

    def __init__(self):
        self.pipelines: List[ProcessingPipeline] = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def run_all(self, data_list: List[Any]) -> None:

        for pipeline, data in zip(self.pipelines, data_list):
            try:
                result = pipeline.process(data)
                print(result)
            except Exception as e:
                print("Pipeline error:", e)


if __name__ == "__main__":

    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")

    manager = NexusManager()

    json_pipeline = JSONAdapter("JSON_001")
    csv_pipeline = CSVAdapter("CSV_001")
    stream_pipeline = StreamAdapter("STREAM_001")

    manager.add_pipeline(json_pipeline)
    manager.add_pipeline(csv_pipeline)
    manager.add_pipeline(stream_pipeline)

    data_samples = [
        '{"sensor": "temp", "value": 23.5}',
        "user,login,123456",
        [22.1, 23.0, 21.9]
    ]

    manager.run_all(data_samples)
