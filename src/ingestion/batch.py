"""
Batch Ingestion Pipeline for IT Data

Handles batch ingestion of sales and expense data from ManOxCo
"""

from pathlib import Path
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp
from src.ingestion import IngestionConfig, IngestionPipeline
from src.ingestion.validators import DataValidator
from src.ingestion.profiler import DataProfiler
from src.logging import setup_logging

logger = setup_logging(__name__)


class BatchIngestionPipeline(IngestionPipeline):
    """Enhanced batch ingestion pipeline with validation and profiling"""
    
    def __init__(
        self,
        spark: SparkSession,
        config: IngestionConfig,
        validator: Optional[DataValidator] = None,
        profiler: Optional[DataProfiler] = None,
    ):
        """
        Initialize batch ingestion pipeline
        
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
    
    def read(self) -> DataFrame:
        """
        Read CSV file from source
        
        Returns:
            DataFrame with raw data
        """
        logger.info(f"Reading CSV from {self.config.source_path}")
        
        df = (
            self.spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(self.config.source_path)
        )
        
        logger.info(f"Read {df.count()} rows from {self.config.source_path}")
        return df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply transformations to add ingestion metadata
        
        Args:
            df: Raw DataFrame
        
        Returns:
            Transformed DataFrame with metadata
        """
        logger.info(f"Transforming data for {self.config.target_layer} layer")
        
        # Add ingestion metadata
        df = (
            df
            .withColumn("source_system", lit(self.config.source_format))
            .withColumn("source_path", lit(self.config.source_path))
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("ingestion_batch_id", lit(Path(self.config.source_path).stem))
        )
        
        logger.info(f"Applied {4} metadata columns")
        return df
    
    def write(self, df: DataFrame) -> None:
        """
        Write to Bronze layer as Parquet
        
        Args:
            df: Transformed DataFrame
        """
        logger.info(f"Writing to {self.config.target_path} with mode {self.config.mode}")
        
        # Create target directory
        target_path = Path(self.config.target_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write with partitioning if specified
        if self.config.partition_cols:
            (
                df.write
                .mode(self.config.mode)
                .partitionBy(*self.config.partition_cols)
                .parquet(str(target_path))
            )
        else:
            (
                df.write
                .mode(self.config.mode)
                .parquet(str(target_path))
            )
        
        logger.info(f"Wrote data to {self.config.target_path}")
    
    def validate(self, df: DataFrame, validation_config: Dict[str, Any]) -> bool:
        """
        Validate data quality
        
        Args:
            df: DataFrame to validate
            validation_config: Validation rules
        
        Returns:
            True if all validations pass
        """
        logger.info(f"Starting validation for {self.config.source_path}")
        
        results = self.validator.validate(
            df,
            source_name=Path(self.config.source_path).stem,
            validation_config=validation_config,
        )
        
        all_passed = all(r.passed for r in results)
        
        if all_passed:
            logger.info("✓ All validations passed")
        else:
            logger.warning("✗ Some validations failed")
            for result in results:
                if not result.passed:
                    logger.warning(f"  - {result.check_name}: {result.details}")
        
        return all_passed
    
    def profile(self, df: DataFrame) -> None:
        """
        Profile data quality
        
        Args:
            df: DataFrame to profile
        """
        logger.info(f"Starting profiling for {self.config.source_path}")
        
        profile = self.profiler.profile(
            df,
            source_name=Path(self.config.source_path).stem,
        )
        
        logger.info(
            f"Data quality score: {profile.data_quality_score:.2%}, "
            f"Duplicate rows: {profile.duplicate_rows}"
        )
    
    def execute(self) -> None:
        """Execute the full ingestion pipeline with validation and profiling"""
        logger.info(f"Starting batch ingestion pipeline for {self.config.source_path}")
        
        try:
            # Read
            df = self.read()
            
            # Profile raw data
            self.profile(df)
            
            # Transform
            df = self.transform(df)
            
            # Validate (optional, depends on validation_rules)
            if self.validation_rules:
                self.validate(df, self.validation_rules)
            
            # Write
            self.write(df)
            
            logger.info("✓ Batch ingestion completed successfully")
        except Exception as e:
            logger.error(f"✗ Batch ingestion failed: {e}", exc_info=True)
            raise


class SalesDataIngestionPipeline(BatchIngestionPipeline):
    """Specialized pipeline for sales data"""
    
    def __init__(self, spark: SparkSession, config: IngestionConfig):
        """Initialize sales data ingestion"""
        super().__init__(spark, config)
        
        # Set validation rules for sales data
        self.validation_rules = {
            "expected_columns": [
                "customer_id", "order_date", "product_id", "quantity",
                "unit_price", "total_amount", "region"
            ],
            "required_columns": ["customer_id", "order_date", "product_id"],
            "key_columns": ["customer_id", "order_date"],
            "range_checks": {
                "quantity": (0, 10000),
                "unit_price": (0, 100000),
                "total_amount": (0, 1000000),
            },
            "min_completeness": 0.95,
        }


class ExpenseDataIngestionPipeline(BatchIngestionPipeline):
    """Specialized pipeline for expense data"""
    
    def __init__(self, spark: SparkSession, config: IngestionConfig):
        """Initialize expense data ingestion"""
        super().__init__(spark, config)
        
        # Set validation rules for expense data
        self.validation_rules = {
            "expected_columns": [
                "expense_id", "expense_date", "category", "amount",
                "department", "description"
            ],
            "required_columns": ["expense_id", "expense_date", "category"],
            "key_columns": ["expense_id"],
            "range_checks": {
                "amount": (0, 5000000),
            },
            "min_completeness": 0.95,
        }


__all__ = [
    "BatchIngestionPipeline",
    "SalesDataIngestionPipeline",
    "ExpenseDataIngestionPipeline",
]
