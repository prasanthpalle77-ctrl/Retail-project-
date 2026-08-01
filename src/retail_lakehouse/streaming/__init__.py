"""File-backed Structured Streaming for local and CI lakehouse execution."""

from retail_lakehouse.streaming.processor import StreamBatchResult, process_stream_batch
from retail_lakehouse.streaming.query import StreamingQueryConfig, start_file_stream
from retail_lakehouse.streaming.specs import STREAMING_DATASETS, stream_schema_ddl

__all__ = [
    "STREAMING_DATASETS",
    "StreamBatchResult",
    "StreamingQueryConfig",
    "process_stream_batch",
    "start_file_stream",
    "stream_schema_ddl",
]
