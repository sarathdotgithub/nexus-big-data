"""
Medallion Architecture Implementation for PI-1 Platform

Implements Bronze, Silver, and Gold data layers with transformations.
"""

from abc import ABC, abstractmethod
from typing import Optional, List, Dict
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col,
    when,
    trim,
    upper,
    coalesce,
    row_number,
    current_timestamp,
    to_timestamp,
)
from datetime import datetime


class MedallionLayer(ABC):
    """Base class for Medallion architecture layers"""
    
    def __init__(self, spark: SparkSession, layer_path: str):
        """
        Initialize medallion layer
        
        Args:
            spark: Spark session
            layer_path: Path to layer storage
        """
        self.spark = spark
        self.layer_path = layer_path
    
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform data at this layer"""
        pass
    
    @abstractmethod
    def write(self, df: DataFrame, table_name: str) -> None:
        """Write data to this layer"""
        pass


class BronzeLayer(MedallionLayer):
    """
    Bronze Layer: Raw, immutable data lake
    
    Characteristics:
    - Stores raw, unmodified data from source systems
    - Maintains complete audit trail (ingestion timestamp, source)
    - Append-only storage
    - Single source of truth for raw data
    """
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Minimal transformation - just add ingestion metadata"""
        return df.withColumn("bronze_ingestion_ts", current_timestamp())
    
    def write(self, df: DataFrame, table_name: str) -> None:
        """Write to Bronze with append mode"""
        output_path = f"{self.layer_path}/bronze_{table_name}"
        df.write.mode("append").parquet(output_path)


class SilverLayer(MedallionLayer):
    """
    Silver Layer: Cleansed and standardized data
    
    Characteristics:
    - Data quality validation (completeness, format)
    - Standardized naming and formatting
    - Business key harmonization
    - Deduplication and null handling
    - Data type standardization
    """
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Apply data cleansing and standardization"""
        
        # Step 1: Standardize column names
        df = df.select([col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])
        
        # Step 2: Trim whitespace and standardize strings
        string_cols = [field.name for field in df.schema.fields if field.dataType.simpleString() == "string"]
        for col_name in string_cols:
            df = df.withColumn(col_name, trim(col(col_name)))
        
        # Step 3: Add quality metrics
        df = df.withColumn("data_quality_score", self._calculate_quality_score(df))
        
        # Step 4: Add silver processing timestamp
        df = df.withColumn("silver_processed_ts", current_timestamp())
        
        return df
    
    def _calculate_quality_score(self, df: DataFrame) -> DataFrame:
        """Calculate data quality score (0-100)"""
        # Simple implementation: percentage of non-null columns
        total_cols = len(df.columns)
        null_counts = [
            when(col(c).isNull(), 1).otherwise(0) for c in df.columns
        ]
        return 100 - (sum(null_counts) * 100 / total_cols)
    
    def write(self, df: DataFrame, table_name: str) -> None:
        """Write to Silver with deduplication"""
        output_path = f"{self.layer_path}/silver_{table_name}"
        
        # Deduplicate based on all columns
        df_dedup = df.dropDuplicates()
        
        df_dedup.write.mode("overwrite").parquet(output_path)


class GoldLayer(MedallionLayer):
    """
    Gold Layer: Business-ready analytics data
    
    Characteristics:
    - Dimensional modeling (facts, dimensions)
    - Pre-aggregated metrics
    - Optimized for BI and analytics queries
    - Serves AI decision copilot
    - Strict schema enforcement
    """
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform to analytics-ready format"""
        
        # Apply domain-specific transformations
        # This is implemented in specific fact/dimension classes
        return df.withColumn("gold_processed_ts", current_timestamp())
    
    def write(self, df: DataFrame, table_name: str) -> None:
        """Write to Gold with partitioning"""
        output_path = f"{self.layer_path}/gold_{table_name}"
        
        # Partition by date for efficient querying
        if "date" in df.columns:
            df.write.mode("overwrite").partitionBy("date").parquet(output_path)
        else:
            df.write.mode("overwrite").parquet(output_path)


class MedallionPipeline:
    """Manages end-to-end Medallion transformations"""
    
    def __init__(self, spark: SparkSession, data_lake_path: str):
        """
        Initialize Medallion pipeline
        
        Args:
            spark: Spark session
            data_lake_path: Base path for data lake
        """
        self.spark = spark
        self.data_lake_path = data_lake_path
        self.bronze = BronzeLayer(spark, data_lake_path)
        self.silver = SilverLayer(spark, data_lake_path)
        self.gold = GoldLayer(spark, data_lake_path)
    
    def bronze_to_silver(self, bronze_table: str) -> DataFrame:
        """Transform Bronze to Silver"""
        bronze_path = f"{self.data_lake_path}/bronze_{bronze_table}"
        df = self.spark.read.parquet(bronze_path)
        df_silver = self.silver.transform(df)
        return df_silver
    
    def silver_to_gold(self, silver_table: str, transformer=None) -> DataFrame:
        """Transform Silver to Gold"""
        silver_path = f"{self.data_lake_path}/silver_{silver_table}"
        df = self.spark.read.parquet(silver_path)
        
        if transformer:
            df = transformer(df)
        else:
            df = self.gold.transform(df)
        
        return df
    
    def create_fact_tables(self) -> None:
        """Create fact tables in Gold layer"""
        # These would be implemented based on specific business logic
        pass
    
    def create_dimension_tables(self) -> None:
        """Create dimension tables in Gold layer"""
        # These would be implemented based on specific business logic
        pass


# Fact Table Definitions
class FactProduction:
    """Fact table for plant production"""
    
    SCHEMA = {
        "production_id": "string",
        "plant_id": "string",
        "production_date": "date",
        "production_quantity": "decimal(18,2)",
        "storage_level": "decimal(18,2)",
        "dispatch_quantity": "decimal(18,2)",
        "production_efficiency": "decimal(5,2)",
        "gold_processed_ts": "timestamp",
    }


class FactDelivery:
    """Fact table for LOX deliveries"""
    
    SCHEMA = {
        "delivery_id": "string",
        "truck_id": "string",
        "origin_plant_id": "string",
        "dest_hospital_id": "string",
        "delivery_date": "date",
        "delivery_quantity": "decimal(18,2)",
        "delivery_duration_hours": "decimal(5,2)",
        "delivery_status": "string",
        "gold_processed_ts": "timestamp",
    }


class FactConsumption:
    """Fact table for hospital consumption"""
    
    SCHEMA = {
        "consumption_id": "string",
        "hospital_id": "string",
        "consumption_date": "date",
        "daily_consumption": "decimal(18,2)",
        "storage_level_eod": "decimal(18,2)",
        "autonomy_hours": "decimal(8,2)",
        "gold_processed_ts": "timestamp",
    }


# Dimension Table Definitions
class DimPlant:
    """Dimension table for plants"""
    
    SCHEMA = {
        "plant_id": "string",
        "plant_name": "string",
        "location": "string",
        "max_production_capacity": "decimal(18,2)",
        "max_storage_capacity": "decimal(18,2)",
        "last_maintenance_date": "date",
        "next_maintenance_date": "date",
    }


class DimHospital:
    """Dimension table for hospitals"""
    
    SCHEMA = {
        "hospital_id": "string",
        "hospital_name": "string",
        "location": "string",
        "storage_capacity": "decimal(18,2)",
        "replenishment_frequency": "string",
        "criticality_level": "string",
    }


class DimTruck:
    """Dimension table for trucks"""
    
    SCHEMA = {
        "truck_id": "string",
        "capacity": "decimal(18,2)",
        "home_plant_id": "string",
        "active": "boolean",
    }


__all__ = [
    "MedallionLayer",
    "BronzeLayer",
    "SilverLayer",
    "GoldLayer",
    "MedallionPipeline",
    "FactProduction",
    "FactDelivery",
    "FactConsumption",
    "DimPlant",
    "DimHospital",
    "DimTruck",
]
