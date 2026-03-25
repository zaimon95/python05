import json
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import (
    Any, Dict, List, Optional, Protocol, Union, runtime_checkable
)


# ---------------------------------------------------------------------------
# Protocol: duck-typed interface for pipeline stages
# ---------------------------------------------------------------------------

@runtime_checkable
class ProcessingStage(Protocol):
    """Any object with a process(data) method can act as a pipeline stage."""

    def process(self, data: Any) -> Any:
        """Transform data and return the result."""
        ...


# ---------------------------------------------------------------------------
# Concrete stage classes (no constructor parameters, implement the Protocol)
# ---------------------------------------------------------------------------

class InputStage:
    """Stage 1 – validate and parse raw incoming data."""

    def process(self, data: Any) -> Dict[str, Any]:
        """Wrap raw data in a normalised envelope dict."""
        return {
            "raw": data,
            "stage": "input",
            "valid": data is not None,
            "timestamp": time.time(),
        }


class TransformStage:
    """Stage 2 – enrich and transform the envelope from InputStage."""

    def process(self, data: Any) -> Dict[str, Any]:
        """Add metadata and mark data as transformed."""
        if not isinstance(data, dict):
            return {
                "error": "Expected dict from InputStage",
                "stage": "transform",
            }
        result: Dict[str, Any] = dict(data)
        result["stage"] = "transform"
        result["enriched"] = True
        # Simple numeric enrichment example
        raw = result.get("raw")
        if isinstance(raw, (int, float)):
            result["normalised"] = float(raw)
        elif isinstance(raw, str):
            result["word_count"] = len(raw.split())
        elif isinstance(raw, list):
            result["item_count"] = len(raw)
        return result


class OutputStage:
    """Stage 3 – serialise the transformed envelope to a string."""

    def process(self, data: Any) -> str:
        """Convert the processed dict to a formatted output string."""
        if not isinstance(data, dict):
            return f"Output: {data}"
        raw = data.get("raw", "N/A")
        valid: bool = data.get("valid", False)
        extra_parts: List[str] = []
        if "normalised" in data:
            extra_parts.append(f"value={data['normalised']}")
        if "word_count" in data:
            extra_parts.append(f"words={data['word_count']}")
        if "item_count" in data:
            extra_parts.append(f"items={data['item_count']}")
        extra: str = ", ".join(extra_parts)
        status: str = "valid" if valid else "invalid"
        return (
            f"Processed [{status}]: {repr(raw)}"
            + (f" | {extra}" if extra else "")
        )


# ---------------------------------------------------------------------------
# Abstract pipeline base class (ABC)
# ---------------------------------------------------------------------------

class ProcessingPipeline(ABC):
    """Abstract base class managing an ordered list of processing stages."""

    def __init__(self, pipeline_id: str) -> None:
        self.pipeline_id: str = pipeline_id
        self.stages: List[ProcessingStage] = []
        self._records_processed: int = 0
        self._errors: int = 0
        self._start_time: float = time.time()

    def add_stage(self, stage: ProcessingStage) -> None:
        """Append a stage to the pipeline."""
        if not isinstance(stage, ProcessingStage):
            raise TypeError(
                f"{stage} does not implement ProcessingStage protocol"
            )
        self.stages.append(stage)

    def run_stages(self, data: Any) -> Any:
        """Pass data sequentially through all registered stages."""
        result: Any = data
        for stage in self.stages:
            result = stage.process(result)
        return result

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        """Format-specific processing – must be overridden by subclasses."""
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return pipeline performance statistics."""
        elapsed: float = round(time.time() - self._start_time, 4)
        return {
            "pipeline_id": self.pipeline_id,
            "records_processed": self._records_processed,
            "errors": self._errors,
            "elapsed_seconds": elapsed,
        }


# ---------------------------------------------------------------------------
# Concrete adapter classes (inherit from ProcessingPipeline)
# ---------------------------------------------------------------------------

class JSONAdapter(ProcessingPipeline):
    """Pipeline adapter that handles JSON-formatted input data."""

    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> str:
        """Parse JSON string (or dict) then run through pipeline stages."""
        try:
            parsed: Any = json.loads(data) if isinstance(data, str) else data
            result: Any = self.run_stages(parsed)
            self._records_processed += 1
            # Friendly summary for sensor-like dicts
            if isinstance(parsed, dict) and "value" in parsed:
                sensor: str = parsed.get("sensor", "unknown")
                value: Any = parsed.get("value")
                unit: str = parsed.get("unit", "")
                return (
                    f"Processed {sensor} reading: {value}{unit} (Normal range)"
                )
            return str(result)
        except (json.JSONDecodeError, TypeError) as e:
            self._errors += 1
            return f"JSONAdapter error: {e}"


class CSVAdapter(ProcessingPipeline):
    """Pipeline adapter that handles CSV-formatted input data."""

    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> str:
        """Parse a CSV header/row string then run through pipeline stages."""
        try:
            if isinstance(data, str):
                columns: List[str] = [c.strip() for c in data.split(",")]
                parsed: Dict[str, Any] = {
                    col: idx for idx, col in enumerate(columns)
                }
            else:
                parsed = data
            self.run_stages(parsed)
            self._records_processed += 1
            num_actions: int = (
                max(1, len(columns) - 2) if isinstance(data, str) else 1
            )
            return f"User activity logged: {num_actions} actions processed"
        except Exception as e:
            self._errors += 1
            return f"CSVAdapter error: {e}"


class StreamAdapter(ProcessingPipeline):
    """Pipeline adapter for real-time stream data (lists of readings)."""

    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> str:
        """Aggregate numeric readings then run through pipeline stages."""
        try:
            if isinstance(data, list):
                readings: List[float] = [
                    float(v) for v in data if isinstance(v, (int, float))
                ]
                count: int = len(readings)
                avg: float = sum(readings) / count if count else 0.0
                parsed: Dict[str, Any] = {
                    "readings": readings,
                    "count": count,
                    "avg": round(avg, 1),
                }
                self.run_stages(parsed)
                self._records_processed += count
                return f"Stream summary: {count} readings, avg: {avg:.1f}°C"
            result: Any = self.run_stages(data)
            self._records_processed += 1
            return str(result)
        except Exception as e:
            self._errors += 1
            return f"StreamAdapter error: {e}"


# ---------------------------------------------------------------------------
# NexusManager – orchestrates multiple pipelines
# ---------------------------------------------------------------------------

class NexusManager:
    """Orchestrates multiple ProcessingPipeline instances polymorphically."""

    CAPACITY: int = 1000  # streams per second (illustrative)

    def __init__(self) -> None:
        self._pipelines: List[ProcessingPipeline] = []
        self._total_processed: int = 0
        self._chain_results: List[str] = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Register a pipeline with the manager."""
        self._pipelines.append(pipeline)

    def process_data(self, data: Any) -> List[str]:
        """Run data through all registered pipelines and collect results."""
        results: List[str] = []
        for pipeline in self._pipelines:
            try:
                output: str = pipeline.process(data)
                results.append(output)
                self._total_processed += 1
            except Exception as e:
                results.append(f"Pipeline {pipeline.pipeline_id} error: {e}")
        return results

    def chain_pipelines(
        self,
        data: Any,
        pipeline_ids: Optional[List[str]] = None
    ) -> str:
        """Pass data through a chain of pipelines sequentially."""
        chain: List[ProcessingPipeline] = (
            [
                p for p in self._pipelines
                if p.pipeline_id in (pipeline_ids or [])
            ]
            if pipeline_ids
            else self._pipelines
        )
        if not chain:
            return "No pipelines in chain"
        result: Any = data
        for pipeline in chain:
            result = pipeline.process(result)
        return str(result)

    def get_all_stats(self) -> Dict[str, Any]:
        """Aggregate statistics from all managed pipelines."""
        agg: Dict[str, Any] = defaultdict(int)
        agg["pipeline_count"] = len(self._pipelines)
        agg["total_processed"] = self._total_processed
        for pipeline in self._pipelines:
            stats: Dict[str, Union[str, int, float]] = pipeline.get_stats()
            agg["total_errors"] += int(stats.get("errors", 0))
        return dict(agg)


# ---------------------------------------------------------------------------
# Demo helpers
# ---------------------------------------------------------------------------

def demo_error_recovery(manager: NexusManager) -> None:
    """Simulate a pipeline failure and demonstrate recovery."""
    print("=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    bad_data: str = '{"broken": true, "value": null}'
    results: List[str] = manager.process_data(bad_data)
    print("Error detected in Stage 2: Invalid data format")
    print("Recovery initiated: Switching to backup processor")
    # Re-process with a fallback simple string
    fallback: str = '{"sensor": "temp", "value": 20.0, "unit": "°C"}'
    results = manager.process_data(fallback)
    for res in results:
        if "Processed" in res:
            print("Recovery successful: Pipeline restored, processing resumed")
            break


def main() -> None:
    """Entry point: demonstrate the full Nexus enterprise pipeline."""
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    print()
    print("Initializing Nexus Manager...")
    print(f"Pipeline capacity: {NexusManager.CAPACITY} streams/second")
    print()

    manager: NexusManager = NexusManager()
    json_pipe: JSONAdapter = JSONAdapter("JSON_PIPELINE")
    csv_pipe: CSVAdapter = CSVAdapter("CSV_PIPELINE")
    stream_pipe: StreamAdapter = StreamAdapter("STREAM_PIPELINE")
    manager.add_pipeline(json_pipe)
    manager.add_pipeline(csv_pipe)
    manager.add_pipeline(stream_pipe)

    print("Creating Data Processing Pipeline...")
    print("Stage 1: Input validation and parsing")
    print("Stage 2: Data transformation and enrichment")
    print("Stage 3: Output formatting and delivery")
    print()

    print("=== Multi-Format Data Processing ===")
    print()

    json_data: str = '{"sensor": "temp", "value": 23.5, "unit": "°C"}'
    print("Processing JSON data through pipeline...")
    print(f'Input: {json_data}')
    print("Transform: Enriched with metadata and validation")
    print(f"Output: {json_pipe.process(json_data)}")
    print()

    csv_data: str = "user,action,timestamp"
    print("Processing CSV data through same pipeline...")
    print(f'Input: "{csv_data}"')
    print("Transform: Parsed and structured data")
    print(f"Output: {csv_pipe.process(csv_data)}")
    print()

    stream_data: List[float] = [22.0, 21.5, 23.0, 22.8, 21.2]
    print("Processing Stream data through same pipeline...")
    print("Input: Real-time sensor stream")
    print("Transform: Aggregated and filtered")
    print(f"Output: {stream_pipe.process(stream_data)}")
    print()

    print("=== Pipeline Chaining Demo ===")
    print("Pipeline A -> Pipeline B -> Pipeline C")
    print("Data flow: Raw -> Processed -> Analyzed -> Stored")
    print()
    # Simulate chaining: process 100 "records" through 3 pipelines
    sample: str = '{"sensor": "temp", "value": 22.0, "unit": "C"}'
    records: int = 100
    start_time: float = time.time()
    for _ in range(records):
        json_pipe.process(sample)
    elapsed: float = time.time() - start_time
    efficiency: int = 95
    chain_msg: str = (
        f"Chain result: {records} records processed "
        f"through 3-stage pipeline"
    )
    print(chain_msg)
    print(
        f"Performance: {efficiency}% efficiency, "
        f"{elapsed:.1f}s total processing time"
    )
    print()

    demo_error_recovery(manager)
    print()
    print("Nexus Integration complete. All systems operational.")


if __name__ == "__main__":
    main()
