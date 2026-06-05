"""
Unit tests for data ingestion pipelines
"""

import pytest
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from src.ingestion import IngestionConfig, BatchIngestionPipeline, ManOxCoIngestionManager
from src.ingestion.validators import (
    SchemaValidator, CompletenessValidator, UniqueKeyValidator,
    RangeValidator, DataValidator
)
from src.ingestion.profiler import DataProfiler


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    return (
        SparkSession.builder
        .appName("test-ingestion")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture
def sample_dataframe(spark):
    """Create a sample DataFrame for testing"""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True),
    ])
    
    data = [
        (1, "Alice", 100.0),
        (2, "Bob", 200.0),
        (3, None, 300.0),
        (4, "David", None),
    ]
    
    return spark.createDataFrame(data, schema=schema)


class TestSchemaValidator:
    """Test schema validation"""
    
    def test_valid_schema(self, sample_dataframe):
        """Test validation with matching schema"""
        validator = SchemaValidator()
        result = validator.validate(
            sample_dataframe,
            {
                "source_name": "test",
                "expected_columns": ["id", "name", "value"]
            }
        )
        
        assert result.passed
        assert result.failed_records == 0
    
    def test_missing_columns(self, sample_dataframe):
        """Test validation with missing columns"""
        validator = SchemaValidator()
        result = validator.validate(
            sample_dataframe,
            {
                "source_name": "test",
                "expected_columns": ["id", "name", "value", "missing_col"]
            }
        )
        
        assert not result.passed
        assert "missing_col" in result.details["missing_columns"]


class TestCompletenessValidator:
    """Test completeness validation"""
    
    def test_valid_completeness(self, sample_dataframe):
        """Test validation with acceptable completeness"""
        validator = CompletenessValidator()
        result = validator.validate(
            sample_dataframe,
            {
                "source_name": "test",
                "required_columns": ["id"],
                "min_completeness": 0.75
            }
        )
        
        assert result.passed


class TestUniqueKeyValidator:
    """Test unique key validation"""
    
    def test_unique_keys(self, sample_dataframe):
        """Test validation with unique keys"""
        validator = UniqueKeyValidator()
        result = validator.validate(
            sample_dataframe,
            {
                "source_name": "test",
                "key_columns": ["id"]
            }
        )
        
        assert result.passed


class TestDataProfiler:
    """Test data profiling"""
    
    def test_profile_basic(self, sample_dataframe):
        """Test basic data profiling"""
        profiler = DataProfiler()
        profile = profiler.profile(sample_dataframe, "test")
        
        assert profile.source_name == "test"
        assert profile.total_records == 4
        assert "id" in profile.column_profiles
    
    def test_quality_score(self, sample_dataframe):
        """Test data quality score calculation"""
        profiler = DataProfiler()
        profile = profiler.profile(sample_dataframe, "test")
        
        assert 0 <= profile.data_quality_score <= 1


class TestManOxCoIngestionManager:
    """Test ManOxCo data source management"""
    
    def test_source_definitions(self):
        """Test that all 6 ManOxCo sources are defined"""
        sources = ManOxCoIngestionManager.SOURCES
        
        expected_sources = {
            "plants_production",
            "lox_truck",
            "lox_delivery",
            "lox_hospital",
            "lox_consumption",
            "plants_data"
        }
        
        assert set(sources.keys()) == expected_sources


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
