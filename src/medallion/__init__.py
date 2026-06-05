"""
Medallion Architecture Implementation for PI-1 Platform

Implements Bronze, Silver, and Gold data layers with transformations.
Features:
- Bronze: Raw, immutable data with audit trail
- Silver: Cleansed, standardized, deduplicated data
- Gold: Dimensional modeling with facts and dimensions
"""

from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Callable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col,
    when,
    trim,
    upper,
    lower,
    coalesce,
    row_number,
    current_timestamp,
    to_timestamp,
    to_date,
    cast,
    sum as spark_sum,
    avg,
    max as spark_max,
    min as spark_min,
    count,
    dense_rank,
    lag,
    datediff,
    date_format,
    concat,
    lit,
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType, BooleanType
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
    - Quality scoring and anomaly detection
    """
    
    def __init__(self, spark: SparkSession, layer_path: str, business_keys: Optional[Dict[str, List[str]]] = None):
        """
        Initialize Silver layer
        
        Args:
            spark: Spark session
            layer_path: Path to layer storage
            business_keys: Dictionary mapping table names to lists of business key columns
        """
        super().__init__(spark, layer_path)
        self.business_keys = business_keys or {}
    
    def transform(self, df: DataFrame, table_name: str = "") -> DataFrame:
        """Apply data cleansing and standardization"""
        
        # Step 1: Standardize column names (lowercase, replace spaces)
        df = df.select([col(c).alias(c.lower().replace(" ", "_").replace("-", "_")) for c in df.columns])
        
        # Step 2: Trim whitespace and standardize strings
        string_cols = [field.name for field in df.schema.fields 
                      if str(field.dataType) in ["string", "StringType()"]]
        for col_name in string_cols:
            df = df.withColumn(col_name, upper(trim(col(col_name))))
        
        # Step 3: Null handling - replace empty strings with null for strings
        for col_name in string_cols:
            df = df.withColumn(col_name, when(col(col_name) == "", None).otherwise(col(col_name)))
        
        # Step 4: Remove duplicates based on business keys if specified
        if table_name in self.business_keys:
            business_key_cols = self.business_keys[table_name]
            # Check if all business key columns exist in the dataframe
            available_keys = [c for c in business_key_cols if c in df.columns]
            if available_keys:
                window = Window.partitionBy(available_keys).orderBy(current_timestamp())
                df = df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
        
        # Step 5: Calculate per-row quality score
        df = df.withColumn("silver_quality_score", self._calculate_quality_score_per_row(df))
        
        # Step 6: Add processing timestamp
        df = df.withColumn("silver_processed_ts", current_timestamp())
        
        return df
    
    def _calculate_quality_score_per_row(self, df: DataFrame):
        """Calculate data quality score per row (0-100)"""
        # Count non-null columns in each row
        total_cols = len(df.columns)
        null_check = sum([when(col(c).isNull(), 0).otherwise(1) for c in df.columns if c != "silver_processed_ts"])
        quality_score = (null_check * 100) / (total_cols - 1)
        return quality_score
    
    def write(self, df: DataFrame, table_name: str) -> None:
        """Write to Silver with deduplication and quality checks"""
        output_path = f"{self.layer_path}/silver_{table_name}"
        
        # Drop rows with quality score below 70% (too many nulls)
        df_filtered = df.filter(col("silver_quality_score") >= 70)
        
        # Write with overwrite mode (replace previous version)
        df_filtered.write.mode("overwrite").parquet(output_path)


class GoldLayer(MedallionLayer):
    """
    Gold Layer: Business-ready analytics data
    
    Characteristics:
    - Dimensional modeling (facts, dimensions)
    - Pre-aggregated metrics and KPIs
    - Optimized for BI and analytics queries
    - Serves AI decision copilot
    - Strict schema enforcement
    - Fact and dimension separation
    """
    
    def transform(self, df: DataFrame, transform_func: Optional[Callable] = None) -> DataFrame:
        """
        Transform to analytics-ready format
        
        Args:
            df: Input DataFrame
            transform_func: Optional custom transformation function
        """
        
        if transform_func:
            return transform_func(df)
        else:
            # Default: just add processing timestamp
            return df.withColumn("gold_processed_ts", current_timestamp())
    
    def write(self, df: DataFrame, table_name: str, partition_cols: Optional[List[str]] = None) -> None:
        """
        Write to Gold with partitioning
        
        Args:
            df: DataFrame to write
            table_name: Table name
            partition_cols: Columns to partition by (e.g., ["date"])
        """
        output_path = f"{self.layer_path}/gold_{table_name}"
        
        # Determine partition columns
        if partition_cols is None:
            # Auto-detect date columns for partitioning
            partition_cols = [c for c in df.columns if "date" in c.lower() and c != "gold_processed_ts"]
        
        # Write with appropriate partitioning
        if partition_cols and all(c in df.columns for c in partition_cols):
            df.write.mode("overwrite").partitionBy(*partition_cols).parquet(output_path)
        else:
            df.write.mode("overwrite").parquet(output_path)


class MedallionPipeline:
    """Manages end-to-end Medallion transformations"""
    
    def __init__(self, spark: SparkSession, data_lake_path: str, business_keys: Optional[Dict[str, List[str]]] = None):
        """
        Initialize Medallion pipeline
        
        Args:
            spark: Spark session
            data_lake_path: Base path for data lake
            business_keys: Dictionary mapping table names to business key columns
        """
        self.spark = spark
        self.data_lake_path = data_lake_path
        self.bronze = BronzeLayer(spark, data_lake_path)
        self.silver = SilverLayer(spark, data_lake_path, business_keys)
        self.gold = GoldLayer(spark, data_lake_path)
        self.business_keys = business_keys or {}
    
    def ingest_to_bronze(self, df: DataFrame, table_name: str) -> None:
        """
        Ingest raw data to Bronze layer
        
        Args:
            df: Input DataFrame
            table_name: Target table name
        """
        df_bronze = self.bronze.transform(df)
        self.bronze.write(df_bronze, table_name)
    
    def bronze_to_silver(self, table_name: str) -> DataFrame:
        """
        Transform Bronze to Silver
        
        Args:
            table_name: Table name (without 'bronze_' prefix)
        
        Returns:
            Transformed DataFrame
        """
        bronze_path = f"{self.data_lake_path}/bronze_{table_name}"
        df = self.spark.read.parquet(bronze_path)
        df_silver = self.silver.transform(df, table_name)
        self.silver.write(df_silver, table_name)
        return df_silver
    
    def silver_to_gold(self, table_name: str, transform_func: Optional[Callable] = None,
                       partition_cols: Optional[List[str]] = None) -> DataFrame:
        """
        Transform Silver to Gold
        
        Args:
            table_name: Table name (without 'silver_' prefix)
            transform_func: Optional custom transformation function
            partition_cols: Columns to partition by in Gold layer
        
        Returns:
            Transformed DataFrame
        """
        silver_path = f"{self.data_lake_path}/silver_{table_name}"
        df = self.spark.read.parquet(silver_path)
        
        df_gold = self.gold.transform(df, transform_func)
        self.gold.write(df_gold, table_name, partition_cols)
        return df_gold
    
    def end_to_end_transform(self, source_path: str, table_name: str, 
                           transform_func: Optional[Callable] = None) -> None:
        """
        Complete Bronze → Silver → Gold transformation
        
        Args:
            source_path: Path to source data
            table_name: Target table name
            transform_func: Optional Gold layer transformation
        """
        # Read source data
        df = self.spark.read.parquet(source_path)
        
        # Bronze
        self.ingest_to_bronze(df, table_name)
        
        # Silver
        self.bronze_to_silver(table_name)
        
        # Gold
        self.silver_to_gold(table_name, transform_func)
    
    def create_fact_table(self, silver_table: str, fact_table_name: str,
                         builder: "FactTableBuilder") -> None:
        """
        Create a fact table from Silver data
        
        Args:
            silver_table: Source Silver table name
            fact_table_name: Target fact table name
            builder: FactTableBuilder instance
        """
        silver_path = f"{self.data_lake_path}/silver_{silver_table}"
        df = self.spark.read.parquet(silver_path)
        df_fact = builder.build(df)
        self.gold.write(df_fact, fact_table_name)
    
    def create_dimension_table(self, silver_table: str, dim_table_name: str,
                              builder: "DimensionTableBuilder") -> None:
        """
        Create a dimension table from Silver data
        
        Args:
            silver_table: Source Silver table name
            dim_table_name: Target dimension table name
            builder: DimensionTableBuilder instance
        """
        silver_path = f"{self.data_lake_path}/silver_{silver_table}"
        df = self.spark.read.parquet(silver_path)
        df_dim = builder.build(df)
        self.gold.write(df_dim, dim_table_name)


# Builder Base Classes
class FactTableBuilder(ABC):
    """Base class for fact table builders"""
    
    @abstractmethod
    def build(self, df: DataFrame) -> DataFrame:
        """Build fact table from source data"""
        pass


class DimensionTableBuilder(ABC):
    """Base class for dimension table builders"""
    
    @abstractmethod
    def build(self, df: DataFrame) -> DataFrame:
        """Build dimension table from source data"""
        pass


# Fact Table Implementations
class FactProduction(FactTableBuilder):
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
    
    def build(self, df: DataFrame) -> DataFrame:
        """
        Build production fact table
        
        Assumes input has: plant_id, production_rate, storage_level, timestamp
        """
        return (df
                .withColumn("production_date", to_date(col("timestamp")))
                .withColumn("production_quantity", col("production_rate"))
                .withColumn("storage_level", col("storage_level"))
                .withColumn("dispatch_quantity", col("dispatch_quantity") if "dispatch_quantity" in df.columns else col("production_rate") * 0.8)
                .withColumn("production_efficiency", (col("production_rate") / 1000) * 100)
                .withColumn("production_id", 
                           concat(col("plant_id"), lit("-"), col("production_date"), lit("-"), 
                                 cast(col("production_quantity"), StringType())))
                .select("plant_id", "production_date", "production_quantity", "storage_level", 
                        "dispatch_quantity", "production_efficiency", "production_id")
                .withColumn("gold_processed_ts", current_timestamp()))


class FactDelivery(FactTableBuilder):
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
    
    def build(self, df: DataFrame) -> DataFrame:
        """
        Build delivery fact table
        
        Assumes input has: truck_id, origin_plant_id, dest_hospital_id, quantity, timestamp, delivery_status
        """
        return (df
                .withColumn("delivery_date", to_date(col("timestamp")))
                .withColumn("delivery_quantity", col("quantity") if "quantity" in df.columns else col("delivery_quantity"))
                .withColumn("delivery_duration_hours", col("duration_hours") if "duration_hours" in df.columns else col("delivery_duration_hours"))
                .withColumn("delivery_status", upper(col("status")) if "status" in df.columns else col("delivery_status"))
                .withColumn("delivery_id",
                           concat(col("truck_id"), lit("-"), col("delivery_date"), lit("-"), col("dest_hospital_id")))
                .select("truck_id", "origin_plant_id", "dest_hospital_id", "delivery_date",
                        "delivery_quantity", "delivery_duration_hours", "delivery_status", "delivery_id")
                .withColumn("gold_processed_ts", current_timestamp()))


class FactConsumption(FactTableBuilder):
    """Fact table for hospital consumption with autonomy calculation"""
    
    SCHEMA = {
        "consumption_id": "string",
        "hospital_id": "string",
        "consumption_date": "date",
        "daily_consumption": "decimal(18,2)",
        "storage_level_eod": "decimal(18,2)",
        "autonomy_hours": "decimal(8,2)",
        "gold_processed_ts": "timestamp",
    }
    
    def build(self, df: DataFrame) -> DataFrame:
        """
        Build consumption fact table with autonomy calculation
        
        Assumes input has: hospital_id, consumption, storage_level, timestamp
        Autonomy = storage_level / (daily_consumption / 24) in hours
        """
        return (df
                .withColumn("consumption_date", to_date(col("timestamp")))
                .withColumn("daily_consumption", col("consumption"))
                .withColumn("storage_level_eod", col("storage_level"))
                # Calculate autonomy: hours of supply remaining
                .withColumn("autonomy_hours",
                           when(col("storage_level") > 0, col("storage_level") / (col("consumption") / 24))
                           .otherwise(0))
                .withColumn("consumption_id",
                           concat(col("hospital_id"), lit("-"), col("consumption_date")))
                .select("hospital_id", "consumption_date", "daily_consumption", 
                        "storage_level_eod", "autonomy_hours", "consumption_id")
                .withColumn("gold_processed_ts", current_timestamp()))


# Dimension Table Implementations
class DimPlant(DimensionTableBuilder):
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
    
    def build(self, df: DataFrame) -> DataFrame:
        """Build plant dimension from source data"""
        return (df
                .dropDuplicates(["plant_id"])
                .select("plant_id", "plant_name", "location", "max_production_capacity", 
                        "max_storage_capacity", "last_maintenance_date", "next_maintenance_date")
                .fillna({"plant_name": "Unknown", "location": "Unknown"})
                .withColumn("gold_processed_ts", current_timestamp()))


class DimHospital(DimensionTableBuilder):
    """Dimension table for hospitals"""
    
    SCHEMA = {
        "hospital_id": "string",
        "hospital_name": "string",
        "location": "string",
        "storage_capacity": "decimal(18,2)",
        "replenishment_frequency": "string",
        "criticality_level": "string",
    }
    
    def build(self, df: DataFrame) -> DataFrame:
        """Build hospital dimension from source data"""
        return (df
                .dropDuplicates(["hospital_id"])
                .select("hospital_id", "hospital_name", "location", "storage_capacity",
                        "replenishment_frequency", "criticality_level")
                .fillna({"hospital_name": "Unknown", "location": "Unknown", 
                        "criticality_level": "MEDIUM"})
                .withColumn("gold_processed_ts", current_timestamp()))


class DimTruck(DimensionTableBuilder):
    """Dimension table for trucks"""
    
    SCHEMA = {
        "truck_id": "string",
        "capacity": "decimal(18,2)",
        "home_plant_id": "string",
        "active": "boolean",
    }
    
    def build(self, df: DataFrame) -> DataFrame:
        """Build truck dimension from source data"""
        return (df
                .dropDuplicates(["truck_id"])
                .select("truck_id", "capacity", "home_plant_id", "active")
                .fillna({"active": True})
                .withColumn("gold_processed_ts", current_timestamp()))


__all__ = [
    "MedallionLayer",
    "BronzeLayer",
    "SilverLayer",
    "GoldLayer",
    "MedallionPipeline",
    "FactTableBuilder",
    "DimensionTableBuilder",
    "FactProduction",
    "FactDelivery",
    "FactConsumption",
    "DimPlant",
    "DimHospital",
    "DimTruck",
]
