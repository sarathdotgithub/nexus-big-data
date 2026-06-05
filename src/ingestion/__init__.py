"""
Data Ingestion Pipelines for PI-1 Platform

Handles batch and streaming ingestion of IT and OT data from ManOxCo sources.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Dict, List
from pathlib import Path
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime


@dataclass
class IngestionConfig:
    """Configuration for ingestion pipelines"""
    source_format: str  # csv, parquet, json, streaming
    source_path: str
    target_layer: str  # bronze
    target_path: str
    schema: Optional[Dict] = None
    partition_cols: Optional[List[str]] = None
    mode: str = "overwrite"  # overwrite, append, ignore, error


class IngestionPipeline(ABC):
    """Base class for ingestion pipelines"""
    
    def __init__(self, spark: SparkSession, config: IngestionConfig):
        """
        Initialize ingestion pipeline
        
        Args:
            spark: Spark session
            config: Ingestion configuration
        """
        self.spark = spark
        self.config = config
    
    @abstractmethod
    def read(self) -> DataFrame:
        """Read data from source"""
        pass
    
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Apply transformations (minimal for Bronze)"""
        pass
    
    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """Write data to target"""
        pass
    
    def execute(self) -> None:
        """Execute the full ingestion pipeline"""
        df = self.read()
        df = self.transform(df)
        self.write(df)


class BatchIngestionPipeline(IngestionPipeline):
    """Batch ingestion pipeline for IT data (sales, expenses)"""
    
    def read(self) -> DataFrame:
        """Read CSV file"""
        return self.spark.read.option("header", "true").csv(self.config.source_path)
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Add ingestion metadata"""
        from pyspark.sql.functions import lit, current_timestamp
        
        return df.withColumn("source_system", lit(self.config.source_format)).withColumn(
            "ingestion_timestamp", current_timestamp()
        )
    
    def write(self, df: DataFrame) -> None:
        """Write to Bronze layer as Parquet"""
        df.write.mode(self.config.mode).parquet(self.config.target_path)


class StreamingIngestionPipeline(IngestionPipeline):
    """Streaming ingestion pipeline for OT data (production, consumption)"""
    
    def read(self) -> DataFrame:
        """Read from streaming source (simulated via socket/file)"""
        # In production, this would read from Kafka, IoT Hub, etc.
        # For now, simulating with structured streaming from files
        return self.spark.readStream.option("header", "true").csv(self.config.source_path)
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Add streaming metadata"""
        from pyspark.sql.functions import lit
        
        return df.withColumn("source_system", lit(self.config.source_format))
    
    def write(self, df: DataFrame) -> None:
        """Write to Bronze layer with checkpointing"""
        (
            df.writeStream.option("checkpointLocation", f"{self.config.target_path}_checkpoint")
            .mode("append")
            .parquet(self.config.target_path)
            .start()
        )


class ManOxCoIngestionManager:
    """Manages ingestion of all ManOxCo data sources"""
    
    # Data source definitions
    SOURCES = {
        "plants_production": {
            "description": "Plant production, storage, and dispatch data",
            "type": "batch",
            "frequency": "daily",
            "table": "bronze_plants_production",
        },
        "lox_truck": {
            "description": "Truck fleet data",
            "type": "batch",
            "frequency": "daily",
            "table": "bronze_lox_truck",
        },
        "lox_delivery": {
            "description": "LOX delivery records",
            "type": "batch",
            "frequency": "daily",
            "table": "bronze_lox_delivery",
        },
        "lox_hospital": {
            "description": "Hospital reference data",
            "type": "batch",
            "frequency": "weekly",
            "table": "bronze_lox_hospital",
        },
        "lox_consumption": {
            "description": "Hospital daily consumption",
            "type": "streaming",
            "frequency": "real-time (1 min)",
            "table": "bronze_lox_consumption",
        },
        "plants_data": {
            "description": "Plant operational data",
            "type": "batch",
            "frequency": "daily",
            "table": "bronze_plants_data",
        },
    }
    
    def __init__(self, spark: SparkSession, data_lake_path: str):
        """
        Initialize ingestion manager
        
        Args:
            spark: Spark session
            data_lake_path: Base path for data lake
        """
        self.spark = spark
        self.data_lake_path = data_lake_path
    
    def ingest_all(self, raw_data_path: str) -> None:
        """
        Ingest all ManOxCo data sources
        
        Args:
            raw_data_path: Path to raw CSV files
        """
        for source_name, source_info in self.SOURCES.items():
            self._ingest_source(source_name, source_info, raw_data_path)
    
    def _ingest_source(self, source_name: str, source_info: Dict, raw_data_path: str) -> None:
        """
        Ingest a single data source
        
        Args:
            source_name: Name of the source
            source_info: Source metadata
            raw_data_path: Path to raw data
        """
        csv_file = Path(raw_data_path) / f"{source_name}.csv"
        
        if not csv_file.exists():
            print(f"Warning: {csv_file} not found, skipping")
            return
        
        config = IngestionConfig(
            source_format="csv",
            source_path=str(csv_file),
            target_layer="bronze",
            target_path=f"{self.data_lake_path}/bronze/{source_info['table']}",
        )
        
        if source_info["type"] == "batch":
            pipeline = BatchIngestionPipeline(self.spark, config)
        else:
            pipeline = StreamingIngestionPipeline(self.spark, config)
        
        try:
            pipeline.execute()
            print(f"✓ Ingested {source_name} to {config.target_path}")
        except Exception as e:
            print(f"✗ Failed to ingest {source_name}: {e}")


__all__ = [
    "IngestionConfig",
    "IngestionPipeline",
    "BatchIngestionPipeline",
    "StreamingIngestionPipeline",
    "ManOxCoIngestionManager",
]
