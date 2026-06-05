"""
Integration tests for data ingestion pipelines
"""

import pytest
import tempfile
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from src.config import PlatformConfig, DataLakeConfig
from src.ingestion import IngestionConfig, ManOxCoIngestionManager
from src.ingestion.batch import BatchIngestionPipeline, SalesDataIngestionPipeline
from src.ingestion.profiler import DataProfiler


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for integration tests"""
    return (
        SparkSession.builder
        .appName("test-ingestion-integration")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def temp_data_lake():
    """Create temporary data lake directories"""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_path = Path(tmpdir)
        bronze_path = base_path / "bronze"
        silver_path = base_path / "silver"
        gold_path = base_path / "gold"
        raw_path = base_path / "raw"
        
        bronze_path.mkdir(parents=True, exist_ok=True)
        silver_path.mkdir(parents=True, exist_ok=True)
        gold_path.mkdir(parents=True, exist_ok=True)
        raw_path.mkdir(parents=True, exist_ok=True)
        
        yield {
            "base": str(base_path),
            "bronze": str(bronze_path),
            "silver": str(silver_path),
            "gold": str(gold_path),
            "raw": str(raw_path),
        }


@pytest.fixture
def sample_csv_file(temp_data_lake):
    """Create a sample CSV file for ingestion"""
    raw_path = Path(temp_data_lake["raw"])
    csv_file = raw_path / "test_data.csv"
    
    # Write sample data
    csv_file.write_text(
        "id,name,value,date\n"
        "1,Alice,100.5,2026-01-01\n"
        "2,Bob,200.3,2026-01-02\n"
        "3,Charlie,150.7,2026-01-03\n"
    )
    
    return str(csv_file)


class TestBatchIngestionIntegration:
    """Integration tests for batch ingestion"""
    
    def test_csv_to_parquet_conversion(self, spark, sample_csv_file, temp_data_lake):
        """Test reading CSV and writing as Parquet"""
        config = IngestionConfig(
            source_format="csv",
            source_path=sample_csv_file,
            target_layer="bronze",
            target_path=temp_data_lake["bronze"] + "/test_data",
        )
        
        pipeline = BatchIngestionPipeline(spark, config)
        
        # Execute pipeline
        pipeline.execute()
        
        # Verify output
        result_df = spark.read.parquet(temp_data_lake["bronze"] + "/test_data")
        
        assert result_df.count() == 3
        assert "ingestion_timestamp" in result_df.columns
        assert "source_system" in result_df.columns
    
    def test_data_quality_profiling(self, spark, sample_csv_file, temp_data_lake):
        """Test data profiling during ingestion"""
        config = IngestionConfig(
            source_format="csv",
            source_path=sample_csv_file,
            target_layer="bronze",
            target_path=temp_data_lake["bronze"] + "/profiled_data",
        )
        
        pipeline = BatchIngestionPipeline(spark, config)
        profiler = DataProfiler()
        
        # Read and profile
        df = pipeline.read()
        profile = profiler.profile(df, "test_data")
        
        assert profile.total_records == 3
        assert profile.data_quality_score > 0
        assert "id" in profile.column_profiles
        assert "name" in profile.column_profiles
    
    def test_metadata_injection(self, spark, sample_csv_file, temp_data_lake):
        """Test that ingestion metadata is properly added"""
        config = IngestionConfig(
            source_format="csv",
            source_path=sample_csv_file,
            target_layer="bronze",
            target_path=temp_data_lake["bronze"] + "/metadata_test",
        )
        
        pipeline = BatchIngestionPipeline(spark, config)
        pipeline.execute()
        
        result_df = spark.read.parquet(temp_data_lake["bronze"] + "/metadata_test")
        
        # Check metadata columns exist
        assert "source_system" in result_df.columns
        assert "source_path" in result_df.columns
        assert "ingestion_timestamp" in result_df.columns
        assert "ingestion_batch_id" in result_df.columns
        
        # Check metadata values
        rows = result_df.select("source_system").distinct().collect()
        assert len(rows) == 1
        assert rows[0]["source_system"] == "csv"


class TestMultiSourceIngestion:
    """Test ingestion of multiple data sources"""
    
    def test_manoxco_source_definitions(self):
        """Test that all ManOxCo sources are properly defined"""
        manager = ManOxCoIngestionManager(None, "/data/lake")
        
        expected_sources = 6
        assert len(manager.SOURCES) == expected_sources
        
        # Verify each source has required metadata
        for source_name, source_info in manager.SOURCES.items():
            assert "description" in source_info
            assert "type" in source_info
            assert "frequency" in source_info
            assert "table" in source_info
            
            # Type must be batch or streaming
            assert source_info["type"] in ["batch", "streaming"]
    
    def test_source_types_distribution(self):
        """Test distribution of batch vs streaming sources"""
        manager = ManOxCoIngestionManager(None, "/data/lake")
        
        batch_sources = [
            s for s, info in manager.SOURCES.items()
            if info["type"] == "batch"
        ]
        streaming_sources = [
            s for s, info in manager.SOURCES.items()
            if info["type"] == "streaming"
        ]
        
        # Should have both batch and streaming
        assert len(batch_sources) > 0
        assert len(streaming_sources) > 0
        
        # Consumption should be streaming
        assert "lox_consumption" in streaming_sources
        
        # Production, truck, delivery, hospital, plants should be batch
        assert "plants_production" in batch_sources
        assert "lox_truck" in batch_sources
        assert "lox_delivery" in batch_sources
        assert "lox_hospital" in batch_sources
        assert "plants_data" in batch_sources


class TestDataValidationIntegration:
    """Integration tests for data validation"""
    
    def test_schema_validation_on_ingestion(self, spark, sample_csv_file, temp_data_lake):
        """Test schema validation during ingestion"""
        config = IngestionConfig(
            source_format="csv",
            source_path=sample_csv_file,
            target_layer="bronze",
            target_path=temp_data_lake["bronze"] + "/validated_data",
        )
        
        pipeline = BatchIngestionPipeline(spark, config)
        pipeline.validation_rules = {
            "expected_columns": ["id", "name", "value", "date"],
        }
        
        df = pipeline.read()
        is_valid = pipeline.validate(df, pipeline.validation_rules)
        
        assert is_valid
    
    def test_completeness_check(self, spark, temp_data_lake):
        """Test completeness validation"""
        # Create CSV with some nulls
        raw_path = Path(temp_data_lake["raw"])
        csv_file = raw_path / "incomplete_data.csv"
        
        csv_file.write_text(
            "id,name,value\n"
            "1,Alice,100\n"
            "2,,200\n"
            "3,Charlie,\n"
        )
        
        config = IngestionConfig(
            source_format="csv",
            source_path=str(csv_file),
            target_layer="bronze",
            target_path=temp_data_lake["bronze"] + "/incomplete_test",
        )
        
        pipeline = BatchIngestionPipeline(spark, config)
        pipeline.validation_rules = {
            "required_columns": ["id"],
            "min_completeness": 0.8,
        }
        
        df = pipeline.read()
        is_valid = pipeline.validate(df, pipeline.validation_rules)
        
        # ID column has 100% completeness, should pass
        assert is_valid


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
