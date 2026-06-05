"""
Streaming Ingestion Pipeline for OT Data

Handles streaming ingestion of production and consumption data from ManOxCo
"""

from pathlib import Path
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, to_timestamp
from src.ingestion import IngestionConfig, IngestionPipeline
from src.ingestion.validators import DataValidator
from src.ingestion.profiler import DataProfiler
from src.logging import setup_logging

logger = setup_logging(__name__)


class StreamingIngestionPipeline(IngestionPipeline):
    """Streaming ingestion pipeline for real-time OT data"""
    
    def __init__(
        self,
        spark: SparkSession,
        config: IngestionConfig,
        validator: Optional[DataValidator] = None,
        profiler: Optional[DataProfiler] = None,
    ):
        """
        Initialize streaming ingestion pipeline
        
        Args:
            spark: Spark session
            config: Ingestion configuration
            validator: Data validator instance
            profiler: Data profiler instance
        """
        super().__init__(spark, config)
        self.validator = validator or DataValidator()
        self.profiler = profiler or DataProfiler()
        self.validation_rules: Dict[str, Any] = {}
        self.query = None  # StreamingQuery object
    
    def read(self) -> DataFrame:
        """
        Read from streaming source (simulated via file)
        
        In production, this would read from Kafka, IoT Hub, or similar
        For simulation, we use structured streaming from file source
        
        Returns:
            DataFrame with streaming data
        """
        logger.info(f"Setting up streaming ingestion from {self.config.source_path}")
        
        # Simulated streaming from CSV files
        df = (
            self.spark.readStream
            .option("header", "true")
            .option("inferSchema", "true")
            .option("maxFilesPerTrigger", 1)
            .csv(self.config.source_path)
        )
        
        logger.info(f"Streaming ingestion configured for {self.config.source_path}")
        return df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply transformations to streaming data
        
        Args:
            df: Streaming DataFrame
        
        Returns:
            Transformed streaming DataFrame
        """
        logger.info("Applying transformations to streaming data")
        
        # Add streaming metadata
        df = (
            df
            .withColumn("source_system", lit(self.config.source_format))
            .withColumn("source_path", lit(self.config.source_path))
            .withColumn("streaming_ingestion_timestamp", col("timestamp"))
            .withColumn("processing_timestamp", lit(None))  # Will be set on write
        )
        
        logger.info("Applied streaming metadata columns")
        return df
    
    def write(self, df: DataFrame) -> None:
        """
        Write streaming data to Bronze layer with checkpointing
        
        Args:
            df: Transformed streaming DataFrame
        """
        logger.info(f"Starting streaming write to {self.config.target_path}")
        
        # Create target directory
        target_path = Path(self.config.target_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        checkpoint_path = f"{self.config.target_path}_checkpoint"
        
        try:
            self.query = (
                df.writeStream
                .option("checkpointLocation", checkpoint_path)
                .option("path", str(target_path))
                .mode("append")
                .format("parquet")
                .start()
            )
            
            logger.info(f"Streaming write started with checkpoint at {checkpoint_path}")
            logger.info(f"Query ID: {self.query.id}")
        except Exception as e:
            logger.error(f"Failed to start streaming write: {e}", exc_info=True)
            raise
    
    def stop(self) -> None:
        """Stop the streaming query"""
        if self.query is not None:
            logger.info("Stopping streaming query")
            self.query.stop()
            logger.info("Streaming query stopped")
    
    def wait_for_termination(self, timeout: Optional[int] = None) -> None:
        """
        Wait for streaming query to terminate
        
        Args:
            timeout: Timeout in seconds (None for infinite)
        """
        if self.query is not None:
            logger.info(f"Waiting for streaming query termination (timeout: {timeout}s)")
            self.query.awaitTermination(timeout)
    
    def execute(self) -> None:
        """Execute the streaming ingestion pipeline"""
        logger.info(f"Starting streaming ingestion pipeline for {self.config.source_path}")
        
        try:
            # Read
            df = self.read()
            
            # Transform
            df = self.transform(df)
            
            # Write
            self.write(df)
            
            logger.info("✓ Streaming ingestion pipeline started successfully")
        except Exception as e:
            logger.error(f"✗ Streaming ingestion pipeline failed: {e}", exc_info=True)
            raise


class ProductionDataStreamingPipeline(StreamingIngestionPipeline):
    """Specialized pipeline for production data streaming"""
    
    def __init__(self, spark: SparkSession, config: IngestionConfig):
        """Initialize production data streaming"""
        super().__init__(spark, config)
        
        # Set validation rules for production data
        self.validation_rules = {
            "expected_columns": [
                "plant_id", "timestamp", "production_rate", "storage_level",
                "dispatch_rate", "status"
            ],
            "required_columns": ["plant_id", "timestamp", "production_rate"],
            "key_columns": ["plant_id", "timestamp"],
            "range_checks": {
                "production_rate": (0, 1000),
                "storage_level": (0, 100000),
                "dispatch_rate": (0, 500),
            },
            "min_completeness": 0.99,
        }


class ConsumptionDataStreamingPipeline(StreamingIngestionPipeline):
    """Specialized pipeline for consumption data streaming"""
    
    def __init__(self, spark: SparkSession, config: IngestionConfig):
        """Initialize consumption data streaming"""
        super().__init__(spark, config)
        
        # Set validation rules for consumption data
        self.validation_rules = {
            "expected_columns": [
                "hospital_id", "timestamp", "consumption_rate", "tank_level",
                "autonomy_hours", "status"
            ],
            "required_columns": ["hospital_id", "timestamp", "consumption_rate"],
            "key_columns": ["hospital_id", "timestamp"],
            "range_checks": {
                "consumption_rate": (0, 100),
                "tank_level": (0, 10000),
                "autonomy_hours": (0, 240),
            },
            "min_completeness": 0.99,
        }


class DeliveryDataStreamingPipeline(StreamingIngestionPipeline):
    """Specialized pipeline for delivery data streaming"""
    
    def __init__(self, spark: SparkSession, config: IngestionConfig):
        """Initialize delivery data streaming"""
        super().__init__(spark, config)
        
        # Set validation rules for delivery data
        self.validation_rules = {
            "expected_columns": [
                "delivery_id", "timestamp", "truck_id", "from_plant",
                "to_hospital", "quantity", "status"
            ],
            "required_columns": ["delivery_id", "timestamp", "truck_id"],
            "key_columns": ["delivery_id"],
            "range_checks": {
                "quantity": (0, 5000),
            },
            "min_completeness": 0.98,
        }


__all__ = [
    "StreamingIngestionPipeline",
    "ProductionDataStreamingPipeline",
    "ConsumptionDataStreamingPipeline",
    "DeliveryDataStreamingPipeline",
]
