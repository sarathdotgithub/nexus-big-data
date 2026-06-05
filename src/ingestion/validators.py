"""
Data Validation Framework for Ingestion Layer

Provides schema validation, data quality checks, and anomaly detection
for ManOxCo data sources.
"""

from dataclasses import dataclass
from typing import Optional, Dict, List, Any
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, when, isnan, isnull, min, max, approx_percentile,
    lit, current_timestamp
)
from src.logging import setup_logging

logger = setup_logging(__name__)


@dataclass
class ValidationResult:
    """Result of a data validation check"""
    source_name: str
    check_name: str
    passed: bool
    total_records: int
    failed_records: int
    failure_rate: float
    details: Dict[str, Any]
    timestamp: str


class Validator(ABC):
    """Base class for data validators"""
    
    @abstractmethod
    def validate(self, df: DataFrame, config: Dict) -> ValidationResult:
        """Execute validation check"""
        pass


class SchemaValidator(Validator):
    """Validates data schema against expected structure"""
    
    def validate(self, df: DataFrame, config: Dict) -> ValidationResult:
        """
        Validate DataFrame schema
        
        Args:
            df: DataFrame to validate
            config: Validation config with 'expected_columns' key
        
        Returns:
            ValidationResult
        """
        from datetime import datetime
        
        expected_columns = set(config.get("expected_columns", []))
        actual_columns = set(df.columns)
        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns
        
        passed = len(missing_columns) == 0
        
        return ValidationResult(
            source_name=config.get("source_name", "unknown"),
            check_name="schema_validation",
            passed=passed,
            total_records=df.count(),
            failed_records=len(missing_columns),
            failure_rate=len(missing_columns) / len(expected_columns) if expected_columns else 0,
            details={
                "missing_columns": list(missing_columns),
                "extra_columns": list(extra_columns),
                "expected_columns": list(expected_columns),
                "actual_columns": list(actual_columns),
            },
            timestamp=datetime.now().isoformat(),
        )


class CompletenessValidator(Validator):
    """Validates data completeness (null values)"""
    
    def validate(self, df: DataFrame, config: Dict) -> ValidationResult:
        """
        Validate data completeness
        
        Args:
            df: DataFrame to validate
            config: Validation config with 'required_columns' and 'min_completeness' keys
        
        Returns:
            ValidationResult
        """
        from datetime import datetime
        
        required_columns = config.get("required_columns", df.columns)
        min_completeness = config.get("min_completeness", 0.95)  # 95% default
        total_records = df.count()
        
        completeness_details = {}
        failed_records = 0
        
        for col_name in required_columns:
            null_count = df.filter(col(col_name).isNull()).count()
            completeness = 1 - (null_count / total_records) if total_records > 0 else 0
            completeness_details[col_name] = {
                "completeness": completeness,
                "null_count": null_count,
                "null_percentage": (null_count / total_records * 100) if total_records > 0 else 0,
            }
            if completeness < min_completeness:
                failed_records += null_count
        
        passed = all(
            details["completeness"] >= min_completeness
            for details in completeness_details.values()
        )
        
        return ValidationResult(
            source_name=config.get("source_name", "unknown"),
            check_name="completeness_validation",
            passed=passed,
            total_records=total_records,
            failed_records=failed_records,
            failure_rate=failed_records / (total_records * len(required_columns)) if total_records > 0 else 0,
            details=completeness_details,
            timestamp=datetime.now().isoformat(),
        )


class UniqueKeyValidator(Validator):
    """Validates uniqueness of key columns"""
    
    def validate(self, df: DataFrame, config: Dict) -> ValidationResult:
        """
        Validate unique key constraints
        
        Args:
            df: DataFrame to validate
            config: Validation config with 'key_columns' key
        
        Returns:
            ValidationResult
        """
        from datetime import datetime
        
        key_columns = config.get("key_columns", [])
        total_records = df.count()
        
        if not key_columns:
            return ValidationResult(
                source_name=config.get("source_name", "unknown"),
                check_name="unique_key_validation",
                passed=True,
                total_records=total_records,
                failed_records=0,
                failure_rate=0.0,
                details={"message": "No key columns specified"},
                timestamp=datetime.now().isoformat(),
            )
        
        duplicate_count = (
            df.groupBy(*key_columns)
            .count()
            .filter(col("count") > 1)
            .count()
        )
        
        passed = duplicate_count == 0
        
        return ValidationResult(
            source_name=config.get("source_name", "unknown"),
            check_name="unique_key_validation",
            passed=passed,
            total_records=total_records,
            failed_records=duplicate_count,
            failure_rate=duplicate_count / total_records if total_records > 0 else 0,
            details={
                "key_columns": key_columns,
                "duplicate_key_rows": duplicate_count,
            },
            timestamp=datetime.now().isoformat(),
        )


class RangeValidator(Validator):
    """Validates numeric values are within expected ranges"""
    
    def validate(self, df: DataFrame, config: Dict) -> ValidationResult:
        """
        Validate numeric ranges
        
        Args:
            df: DataFrame to validate
            config: Validation config with 'range_checks' key
        
        Returns:
            ValidationResult
        """
        from datetime import datetime
        
        range_checks = config.get("range_checks", {})  # {column: (min, max)}
        total_records = df.count()
        failed_records = 0
        range_details = {}
        
        for col_name, (min_val, max_val) in range_checks.items():
            out_of_range = df.filter(
                (col(col_name) < min_val) | (col(col_name) > max_val)
            ).count()
            
            range_details[col_name] = {
                "min_expected": min_val,
                "max_expected": max_val,
                "out_of_range_count": out_of_range,
                "out_of_range_percentage": (out_of_range / total_records * 100) if total_records > 0 else 0,
            }
            failed_records += out_of_range
        
        passed = failed_records == 0
        
        return ValidationResult(
            source_name=config.get("source_name", "unknown"),
            check_name="range_validation",
            passed=passed,
            total_records=total_records,
            failed_records=failed_records,
            failure_rate=failed_records / (total_records * len(range_checks)) if total_records > 0 else 0,
            details=range_details,
            timestamp=datetime.now().isoformat(),
        )


class DataValidator:
    """Orchestrates data validation for ingestion"""
    
    def __init__(self):
        """Initialize data validator"""
        self.results: List[ValidationResult] = []
        self.validators = {
            "schema": SchemaValidator(),
            "completeness": CompletenessValidator(),
            "unique_key": UniqueKeyValidator(),
            "range": RangeValidator(),
        }
    
    def validate(
        self,
        df: DataFrame,
        source_name: str,
        validation_config: Dict[str, Any],
    ) -> ValidationResult:
        """
        Run all validations for a data source
        
        Args:
            df: DataFrame to validate
            source_name: Name of data source
            validation_config: Dict with validation checks to run
        
        Returns:
            List of ValidationResult objects
        """
        logger.info(f"Starting validation for {source_name}")
        results = []
        
        validation_config["source_name"] = source_name
        
        for check_type, config in validation_config.items():
            if check_type == "source_name":
                continue
            
            if check_type in self.validators:
                try:
                    result = self.validators[check_type].validate(df, validation_config)
                    results.append(result)
                    
                    status = "✓ PASS" if result.passed else "✗ FAIL"
                    logger.info(
                        f"{status} {check_type} validation for {source_name}: "
                        f"{result.failed_records}/{result.total_records} issues"
                    )
                except Exception as e:
                    logger.error(f"Error in {check_type} validation: {e}")
        
        return results
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate validation report"""
        total_checks = len(self.results)
        passed_checks = sum(1 for r in self.results if r.passed)
        
        return {
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": total_checks - passed_checks,
            "pass_rate": passed_checks / total_checks if total_checks > 0 else 0,
            "results": [
                {
                    "source": r.source_name,
                    "check": r.check_name,
                    "passed": r.passed,
                    "details": r.details,
                }
                for r in self.results
            ],
        }


__all__ = [
    "ValidationResult",
    "Validator",
    "SchemaValidator",
    "CompletenessValidator",
    "UniqueKeyValidator",
    "RangeValidator",
    "DataValidator",
]
