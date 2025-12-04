"""
Three-Tier Storage Architecture for cryptocurrency streaming pipeline.

This module provides a layered storage system:
- Hot Path (Redis): Real-time queries with < 10ms latency
- Warm Path (DuckDB): Interactive analytics with < 1s latency  
- Cold Path (Parquet): Historical archive with unlimited retention

Components:
- RedisStorage: In-memory storage for latest data
- DuckDBStorage: OLAP storage for 90-day analytics
- ParquetStorage: File-based archive storage
- StorageWriter: Multi-tier write coordinator
- QueryRouter: Automatic tier selection for queries
"""

from .redis_storage import RedisStorage
from .duckdb_storage import DuckDBStorage
from .parquet_storage import ParquetStorage
from .postgres_storage import PostgresStorage
from .minio_storage import MinioStorage
from .storage_writer import StorageWriter
from .query_router import QueryRouter

__all__ = [
    "RedisStorage",
    "DuckDBStorage", 
    "ParquetStorage",
    "PostgresStorage",
    "MinioStorage",
    "StorageWriter",
    "QueryRouter",
]
