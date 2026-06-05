"""
Data Profiling Framework for Ingestion Layer

Provides statistical analysis and data quality metrics for ingested data.
"""

from dataclasses import dataclass
from typing import Dict, List, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, mean, stddev, min as spark_min, max as spark_max,
    percentile_approx, when, isnan, isnull, lit, col as spark_col
)
from datetime import datetime
from src.logging import setup_logging

logger = setup_logging(__name__)


@dataclass
class ColumnProfile:
    """Statistical profile of a single column"""
    column_name: str
    data_type: str
    non_null_count: int
    null_count: int
    null_percentage: float
    unique_count: Optional[int]
    unique_percentage: Optional[float]
    # Numeric statistics
    min_value: Optional[float]
    max_value: Optional[float]
    mean_value: Optional[float]
    stddev_value: Optional[float]
    percentile_25: Optional[float]
    percentile_50: Optional[float]
    percentile_75: Optional[float]
    # String statistics
    min_length: Optional[int]
    max_length: Optional[int]
    avg_length: Optional[float]


@dataclass
class DataProfile:
    """Complete profile of a dataset"""
    source_name: str
    total_records: int
    ingestion_timestamp: str
    column_profiles: Dict[str, ColumnProfile]
    duplicate_rows: int
    data_quality_score: float


class DataProfiler:
    """Profiles data characteristics for quality assessment"""
    
    def __init__(self):
        """Initialize data profiler"""
        self.profiles: List[DataProfile] = []
    
    def profile(self, df: DataFrame, source_name: str) -> DataProfile:
        """
        Profile a dataset
        
        Args:
            df: DataFrame to profile
            source_name: Name of data source
        
        Returns:
            DataProfile object
        """
        logger.info(f"Starting profiling for {source_name}")
        
        total_records = df.count()
        column_profiles = {}
        
        for field in df.schema.fields:
            column_profiles[field.name] = self._profile_column(
                df, field.name, field.dataType.typeName()
            )
        
        # Calculate duplicate rows
        duplicate_rows = self._count_duplicate_rows(df)
        
        # Calculate data quality score
        data_quality_score = self._calculate_quality_score(column_profiles)
        
        profile = DataProfile(
            source_name=source_name,
            total_records=total_records,
            ingestion_timestamp=datetime.now().isoformat(),
            column_profiles=column_profiles,
            duplicate_rows=duplicate_rows,
            data_quality_score=data_quality_score,
        )
        
        self.profiles.append(profile)
        logger.info(
            f"Profiling complete for {source_name}: "
            f"{total_records} records, quality score: {data_quality_score:.2%}"
        )
        
        return profile
    
    def _profile_column(self, df: DataFrame, column_name: str, data_type: str) -> ColumnProfile:
        """
        Profile a single column
        
        Args:
            df: DataFrame containing the column
            column_name: Name of the column
            data_type: Data type of the column
        
        Returns:
            ColumnProfile object
        """
        total_records = df.count()
        
        # Null/non-null counts
        null_count = df.filter(col(column_name).isNull()).count()
        non_null_count = total_records - null_count
        null_percentage = (null_count / total_records * 100) if total_records > 0 else 0
        
        # Unique values
        unique_count = df.select(countDistinct(column_name)).collect()[0][0]
        unique_percentage = (unique_count / total_records * 100) if total_records > 0 else 0
        
        # Type-specific statistics
        min_val, max_val, mean_val, stddev_val = None, None, None, None
        p25, p50, p75 = None, None, None
        min_len, max_len, avg_len = None, None, None
        
        if data_type in ["long", "integer", "double", "float", "decimal"]:
            # Numeric statistics
            stats = df.select(
                spark_min(col(column_name)).alias("min"),
                spark_max(col(column_name)).alias("max"),
                mean(col(column_name)).alias("mean"),
                stddev(col(column_name)).alias("stddev"),
                percentile_approx(col(column_name), 0.25).alias("p25"),
                percentile_approx(col(column_name), 0.50).alias("p50"),
                percentile_approx(col(column_name), 0.75).alias("p75"),
            ).collect()[0]
            
            min_val = stats.min
            max_val = stats.max
            mean_val = stats.mean
            stddev_val = stats.stddev
            p25 = stats.p25
            p50 = stats.p50
            p75 = stats.p75
        
        elif data_type in ["string"]:
            # String statistics
            str_stats = df.select(
                spark_min(col(f"length({column_name})")).alias("min_len"),
                spark_max(col(f"length({column_name})")).alias("max_len"),
                mean(col(f"length({column_name})")).alias("avg_len"),
            ).collect()[0]
            
            min_len = str_stats.min_len
            max_len = str_stats.max_len
            avg_len = str_stats.avg_len
        
        return ColumnProfile(
            column_name=column_name,
            data_type=data_type,
            non_null_count=non_null_count,
            null_count=null_count,
            null_percentage=null_percentage,
            unique_count=unique_count,
            unique_percentage=unique_percentage,
            min_value=min_val,
            max_value=max_val,
            mean_value=mean_val,
            stddev_value=stddev_val,
            percentile_25=p25,
            percentile_50=p50,
            percentile_75=p75,
            min_length=min_len,
            max_length=max_len,
            avg_length=avg_len,
        )
    
    def _count_duplicate_rows(self, df: DataFrame) -> int:
        """
        Count completely duplicate rows
        
        Args:
            df: DataFrame to check
        
        Returns:
            Number of duplicate rows
        """
        try:
            total_rows = df.count()
            distinct_rows = df.distinct().count()
            return total_rows - distinct_rows
        except Exception as e:
            logger.warning(f"Could not count duplicates: {e}")
            return 0
    
    def _calculate_quality_score(self, column_profiles: Dict[str, ColumnProfile]) -> float:
        """
        Calculate overall data quality score
        
        Args:
            column_profiles: Dictionary of column profiles
        
        Returns:
            Quality score between 0 and 1
        """
        if not column_profiles:
            return 0.0
        
        # Score based on null percentage (weight: 0.6)
        avg_completeness = sum(
            (1 - (p.null_percentage / 100))
            for p in column_profiles.values()
        ) / len(column_profiles)
        
        # Score based on uniqueness (weight: 0.2)
        avg_uniqueness = sum(
            min(p.unique_percentage / 100, 1.0) if p.unique_percentage else 0
            for p in column_profiles.values()
        ) / len(column_profiles)
        
        # Combined score
        quality_score = (avg_completeness * 0.6) + (avg_uniqueness * 0.2) + 0.2
        
        return min(max(quality_score, 0.0), 1.0)
    
    def generate_report(self, source_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate profiling report
        
        Args:
            source_name: Filter by source name (optional)
        
        Returns:
            Dictionary containing profiling report
        """
        profiles = self.profiles
        if source_name:
            profiles = [p for p in self.profiles if p.source_name == source_name]
        
        if not profiles:
            return {"message": "No profiles found"}
        
        profile = profiles[-1]  # Get latest profile for source
        
        return {
            "source_name": profile.source_name,
            "total_records": profile.total_records,
            "duplicate_rows": profile.duplicate_rows,
            "data_quality_score": profile.data_quality_score,
            "ingestion_timestamp": profile.ingestion_timestamp,
            "columns": {
                col_name: {
                    "data_type": col_profile.data_type,
                    "non_null_count": col_profile.non_null_count,
                    "null_percentage": col_profile.null_percentage,
                    "unique_count": col_profile.unique_count,
                    "unique_percentage": col_profile.unique_percentage,
                    "min": col_profile.min_value,
                    "max": col_profile.max_value,
                    "mean": col_profile.mean_value,
                    "stddev": col_profile.stddev_value,
                    "percentiles": {
                        "25": col_profile.percentile_25,
                        "50": col_profile.percentile_50,
                        "75": col_profile.percentile_75,
                    },
                }
                for col_name, col_profile in profile.column_profiles.items()
            },
        }
    
    def generate_summary(self) -> Dict[str, Any]:
        """
        Generate summary report across all profiles
        
        Returns:
            Dictionary containing summary statistics
        """
        if not self.profiles:
            return {"message": "No profiles found"}
        
        avg_quality_score = sum(p.data_quality_score for p in self.profiles) / len(self.profiles)
        total_records = sum(p.total_records for p in self.profiles)
        total_duplicates = sum(p.duplicate_rows for p in self.profiles)
        
        return {
            "total_profiles": len(self.profiles),
            "total_records_profiled": total_records,
            "average_quality_score": avg_quality_score,
            "total_duplicate_rows": total_duplicates,
            "sources": [
                {
                    "source_name": p.source_name,
                    "records": p.total_records,
                    "quality_score": p.data_quality_score,
                    "ingestion_timestamp": p.ingestion_timestamp,
                }
                for p in self.profiles
            ],
        }


__all__ = [
    "ColumnProfile",
    "DataProfile",
    "DataProfiler",
]
