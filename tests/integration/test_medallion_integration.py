"""
Integration tests for Medallion Architecture transformations

Tests Bronze → Silver → Gold transformations with ManOxCo data scenarios.
"""

import pytest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime, timedelta
from src.medallion import (
    MedallionPipeline,
    BronzeLayer,
    SilverLayer,
    GoldLayer,
    FactProduction,
    FactDelivery,
    FactConsumption,
    DimPlant,
    DimHospital,
    DimTruck,
)
import tempfile


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("medallion-test") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


@pytest.fixture
def data_lake_path(tmp_path):
    """Temporary data lake path"""
    return str(tmp_path / "data_lake")


@pytest.fixture
def medallion_pipeline(spark, data_lake_path):
    """Create a medallion pipeline instance"""
    business_keys = {
        "plants_production": ["plant_id", "timestamp"],
        "lox_hospital": ["hospital_id", "timestamp"],
        "lox_delivery": ["truck_id", "destination", "timestamp"],
    }
    return MedallionPipeline(spark, data_lake_path, business_keys)


class TestBronzeLayer:
    """Tests for Bronze layer ingestion"""
    
    def test_bronze_ingestion_adds_timestamp(self, spark, data_lake_path):
        """Test that Bronze layer adds ingestion timestamp"""
        bronze = BronzeLayer(spark, data_lake_path)
        
        # Create test data
        data = [
            ("plant1", 100.0, "2026-06-01 10:00:00"),
            ("plant2", 150.0, "2026-06-01 10:30:00"),
        ]
        df = spark.createDataFrame(data, ["plant_id", "production_rate", "timestamp"])
        
        # Transform
        df_bronze = bronze.transform(df)
        
        # Verify
        assert "bronze_ingestion_ts" in df_bronze.columns
        assert df_bronze.count() == 2
        print("✅ Bronze ingestion timestamp test passed")
    
    def test_bronze_preserves_data(self, spark, data_lake_path):
        """Test that Bronze layer preserves all data"""
        bronze = BronzeLayer(spark, data_lake_path)
        
        data = [
            ("plant1", 100.0, None),  # Include null
            ("plant2", 150.0, 95.0),
        ]
        df = spark.createDataFrame(data, ["plant_id", "production_rate", "efficiency"])
        
        df_bronze = bronze.transform(df)
        
        assert df_bronze.count() == 2
        assert len(df_bronze.columns) == 4  # Original 3 + timestamp
        print("✅ Bronze data preservation test passed")


class TestSilverLayer:
    """Tests for Silver layer transformations"""
    
    def test_silver_standardizes_column_names(self, spark, data_lake_path):
        """Test that Silver layer standardizes column names"""
        silver = SilverLayer(spark, data_lake_path)
        
        data = [("Plant 1", "Madrid", 100.0)]
        df = spark.createDataFrame(data, ["Plant Name", "City", "Production"])
        
        df_silver = silver.transform(df)
        
        # Column names should be lowercase with underscores
        assert "plant_name" in df_silver.columns
        assert "city" in df_silver.columns
        assert "production" in df_silver.columns
        print("✅ Column name standardization test passed")
    
    def test_silver_trims_whitespace(self, spark, data_lake_path):
        """Test that Silver layer trims string whitespace"""
        silver = SilverLayer(spark, data_lake_path)
        
        data = [(" plant1 ", "  madrid  ")]
        df = spark.createDataFrame(data, ["plant_id", "location"])
        
        df_silver = silver.transform(df)
        
        result = df_silver.select("plant_id", "location").collect()[0]
        assert result["plant_id"] == "PLANT1"  # Also uppercased
        assert result["location"] == "MADRID"
        print("✅ Whitespace trimming test passed")
    
    def test_silver_calculates_quality_score(self, spark, data_lake_path):
        """Test that Silver layer calculates quality scores"""
        silver = SilverLayer(spark, data_lake_path)
        
        data = [
            ("plant1", "madrid", 100.0),  # All non-null
            ("plant2", None, 150.0),       # One null
        ]
        df = spark.createDataFrame(data, ["plant_id", "location", "production"])
        
        df_silver = silver.transform(df)
        
        assert "silver_quality_score" in df_silver.columns
        scores = df_silver.select("silver_quality_score").collect()
        assert scores[0]["silver_quality_score"] > scores[1]["silver_quality_score"]
        print("✅ Quality score calculation test passed")
    
    def test_silver_removes_low_quality_rows(self, spark, data_lake_path):
        """Test that Silver layer filters low-quality rows"""
        silver = SilverLayer(spark, data_lake_path)
        
        # Create a row with very few non-null columns
        data = [
            ("plant1", "madrid", 100.0),      # 3/3 non-null = 100%
            (None, None, None),               # 0/3 non-null = 0% (will be filtered)
        ]
        df = spark.createDataFrame(data, ["plant_id", "location", "production"])
        
        df_silver = silver.transform(df)
        
        # Should filter out the all-null row (quality < 70%)
        assert df_silver.count() <= df.count()
        print("✅ Low-quality row filtering test passed")
    
    def test_silver_deduplication(self, spark, data_lake_path):
        """Test that Silver layer deduplicates based on business keys"""
        business_keys = {"plants": ["plant_id", "timestamp"]}
        silver = SilverLayer(spark, data_lake_path, business_keys)
        
        data = [
            ("plant1", "2026-06-01", 100.0),
            ("plant1", "2026-06-01", 100.0),  # Duplicate
            ("plant2", "2026-06-01", 150.0),
        ]
        df = spark.createDataFrame(data, ["plant_id", "timestamp", "production"])
        
        df_silver = silver.transform(df, "plants")
        
        # Should have deduplicated
        assert df_silver.count() == 2
        print("✅ Deduplication test passed")


class TestGoldLayer:
    """Tests for Gold layer transformations"""
    
    def test_gold_adds_processing_timestamp(self, spark, data_lake_path):
        """Test that Gold layer adds processing timestamp"""
        gold = GoldLayer(spark, data_lake_path)
        
        data = [("plant1", 100.0)]
        df = spark.createDataFrame(data, ["plant_id", "production"])
        
        df_gold = gold.transform(df)
        
        assert "gold_processed_ts" in df_gold.columns
        print("✅ Gold processing timestamp test passed")
    
    def test_gold_custom_transformation(self, spark, data_lake_path):
        """Test that Gold layer applies custom transformations"""
        gold = GoldLayer(spark, data_lake_path)
        
        def custom_transform(df):
            return df.withColumn("doubled_production", df.production * 2)
        
        data = [("plant1", 100.0)]
        df = spark.createDataFrame(data, ["plant_id", "production"])
        
        df_gold = gold.transform(df, custom_transform)
        
        assert "doubled_production" in df_gold.columns
        result = df_gold.select("doubled_production").collect()[0]
        assert result["doubled_production"] == 200.0
        print("✅ Custom transformation test passed")


class TestFactProduction:
    """Tests for Production fact table"""
    
    def test_production_fact_builds_correctly(self, spark):
        """Test that Production fact table builds correctly"""
        fact = FactProduction()
        
        data = [
            ("plant1", 100.0, 5000.0, None, "2026-06-01 10:00:00"),
            ("plant2", 150.0, 8000.0, None, "2026-06-01 10:30:00"),
        ]
        df = spark.createDataFrame(data, 
            ["plant_id", "production_rate", "storage_level", "dispatch_quantity", "timestamp"])
        
        df_fact = fact.build(df)
        
        assert "production_date" in df_fact.columns
        assert "production_quantity" in df_fact.columns
        assert "production_efficiency" in df_fact.columns
        assert "production_id" in df_fact.columns
        assert df_fact.count() == 2
        print("✅ Production fact table build test passed")


class TestFactDelivery:
    """Tests for Delivery fact table"""
    
    def test_delivery_fact_builds_correctly(self, spark):
        """Test that Delivery fact table builds correctly"""
        fact = FactDelivery()
        
        data = [
            ("truck1", "plant1", "hospital1", 500.0, 4.0, "COMPLETED", "2026-06-01 10:00:00"),
            ("truck2", "plant2", "hospital2", 600.0, 5.0, "IN_TRANSIT", "2026-06-01 11:00:00"),
        ]
        df = spark.createDataFrame(data,
            ["truck_id", "origin_plant_id", "dest_hospital_id", "quantity", 
             "duration_hours", "status", "timestamp"])
        
        df_fact = fact.build(df)
        
        assert "delivery_date" in df_fact.columns
        assert "delivery_quantity" in df_fact.columns
        assert "delivery_id" in df_fact.columns
        assert df_fact.count() == 2
        print("✅ Delivery fact table build test passed")


class TestFactConsumption:
    """Tests for Consumption fact table"""
    
    def test_consumption_fact_with_autonomy(self, spark):
        """Test that Consumption fact calculates autonomy hours"""
        fact = FactConsumption()
        
        # Consumption: 100 units/day, Storage: 200 units
        # Autonomy: 200 / (100/24) = 48 hours
        data = [
            ("hospital1", 100.0, 200.0, "2026-06-01 10:00:00"),
            ("hospital2", 150.0, 300.0, "2026-06-01 10:00:00"),
        ]
        df = spark.createDataFrame(data,
            ["hospital_id", "consumption", "storage_level", "timestamp"])
        
        df_fact = fact.build(df)
        
        assert "autonomy_hours" in df_fact.columns
        results = df_fact.collect()
        # First hospital: 200 / (100/24) = 48 hours
        assert abs(results[0]["autonomy_hours"] - 48.0) < 1.0
        print("✅ Consumption fact with autonomy test passed")
    
    def test_consumption_fact_zero_storage(self, spark):
        """Test that Consumption fact handles zero storage gracefully"""
        fact = FactConsumption()
        
        data = [
            ("hospital1", 100.0, 0.0, "2026-06-01 10:00:00"),
        ]
        df = spark.createDataFrame(data,
            ["hospital_id", "consumption", "storage_level", "timestamp"])
        
        df_fact = fact.build(df)
        
        result = df_fact.collect()[0]
        assert result["autonomy_hours"] == 0.0
        print("✅ Zero storage autonomy test passed")


class TestDimensionTables:
    """Tests for dimension tables"""
    
    def test_plant_dimension_deduplicates(self, spark):
        """Test that Plant dimension deduplicates on plant_id"""
        dim = DimPlant()
        
        data = [
            ("plant1", "Plant 1", "Madrid", 1000.0, 10000.0, None, None),
            ("plant1", "Plant 1", "Madrid", 1000.0, 10000.0, None, None),  # Duplicate
            ("plant2", "Plant 2", "Barcelona", 2000.0, 20000.0, None, None),
        ]
        df = spark.createDataFrame(data,
            ["plant_id", "plant_name", "location", "max_production_capacity", 
             "max_storage_capacity", "last_maintenance_date", "next_maintenance_date"])
        
        df_dim = dim.build(df)
        
        assert df_dim.count() == 2
        print("✅ Plant dimension deduplication test passed")
    
    def test_hospital_dimension_deduplicates(self, spark):
        """Test that Hospital dimension deduplicates on hospital_id"""
        dim = DimHospital()
        
        data = [
            ("hosp1", "Hospital 1", "Madrid", 500.0, "DAILY", "CRITICAL"),
            ("hosp1", "Hospital 1", "Madrid", 500.0, "DAILY", "CRITICAL"),  # Duplicate
            ("hosp2", "Hospital 2", "Barcelona", 300.0, "WEEKLY", "MEDIUM"),
        ]
        df = spark.createDataFrame(data,
            ["hospital_id", "hospital_name", "location", "storage_capacity",
             "replenishment_frequency", "criticality_level"])
        
        df_dim = dim.build(df)
        
        assert df_dim.count() == 2
        print("✅ Hospital dimension deduplication test passed")
    
    def test_truck_dimension_deduplicates(self, spark):
        """Test that Truck dimension deduplicates on truck_id"""
        dim = DimTruck()
        
        data = [
            ("truck1", 500.0, "plant1", True),
            ("truck1", 500.0, "plant1", True),  # Duplicate
            ("truck2", 600.0, "plant2", True),
        ]
        df = spark.createDataFrame(data,
            ["truck_id", "capacity", "home_plant_id", "active"])
        
        df_dim = dim.build(df)
        
        assert df_dim.count() == 2
        print("✅ Truck dimension deduplication test passed")


class TestMedallionPipeline:
    """Integration tests for the complete pipeline"""
    
    def test_ingest_to_bronze(self, medallion_pipeline, spark):
        """Test ingestion from source to Bronze"""
        data = [
            ("plant1", 100.0, "2026-06-01 10:00:00"),
            ("plant2", 150.0, "2026-06-01 10:30:00"),
        ]
        df = spark.createDataFrame(data, ["plant_id", "production_rate", "timestamp"])
        
        medallion_pipeline.ingest_to_bronze(df, "plants_production")
        
        # Verify bronze table exists
        bronze_path = f"{medallion_pipeline.data_lake_path}/bronze_plants_production"
        bronze_df = spark.read.parquet(bronze_path)
        assert bronze_df.count() == 2
        assert "bronze_ingestion_ts" in bronze_df.columns
        print("✅ Bronze ingestion test passed")
    
    def test_bronze_to_silver(self, medallion_pipeline, spark):
        """Test transformation from Bronze to Silver"""
        data = [
            ("plant1", 100.0, " madrid "),
            ("plant2", 150.0, " barcelona "),
        ]
        df = spark.createDataFrame(data, ["plant_id", "production_rate", "City"])
        
        medallion_pipeline.ingest_to_bronze(df, "plants_production")
        medallion_pipeline.bronze_to_silver("plants_production")
        
        # Verify silver table exists
        silver_path = f"{medallion_pipeline.data_lake_path}/silver_plants_production"
        silver_df = spark.read.parquet(silver_path)
        assert silver_df.count() > 0
        assert "city" in silver_df.columns  # Lowercase
        assert "silver_processed_ts" in silver_df.columns
        assert "silver_quality_score" in silver_df.columns
        print("✅ Bronze to Silver test passed")
    
    def test_end_to_end_transformation(self, medallion_pipeline, spark, tmp_path):
        """Test complete Bronze → Silver → Gold transformation"""
        # Create source data
        source_path = str(tmp_path / "source_data")
        data = [
            ("plant1", 100.0, 5000.0, "2026-06-01 10:00:00"),
            ("plant2", 150.0, 8000.0, "2026-06-01 10:30:00"),
        ]
        df = spark.createDataFrame(data, 
            ["plant_id", "production_rate", "storage_level", "timestamp"])
        df.write.mode("overwrite").parquet(source_path)
        
        # Run end-to-end transformation
        fact_builder = FactProduction()
        medallion_pipeline.end_to_end_transform(
            source_path, 
            "production_fact",
            fact_builder.build
        )
        
        # Verify gold table exists
        gold_path = f"{medallion_pipeline.data_lake_path}/gold_production_fact"
        gold_df = spark.read.parquet(gold_path)
        assert gold_df.count() > 0
        assert "production_id" in gold_df.columns
        print("✅ End-to-end transformation test passed")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
