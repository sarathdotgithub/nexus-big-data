"""
Governance Framework for PI-1 Platform

Defines data governance policies, quality standards, and operational controls.
"""

from dataclasses import dataclass
from typing import List, Dict
from enum import Enum
from datetime import datetime


class DataClassification(Enum):
    """Data sensitivity classification"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


class DataQualityRule(Enum):
    """Standard data quality rules"""
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE_CHECK = "range_check"
    FORMAT_CHECK = "format_check"
    REFERENTIAL_INTEGRITY = "referential_integrity"


@dataclass
class DataGovernancePolicy:
    """Data governance policy definition"""
    
    # Naming Conventions
    NAMING_CONVENTIONS = {
        "table_prefix_bronze": "bronze_",
        "table_prefix_silver": "silver_",
        "table_prefix_gold": "gold_",
        "dimension_prefix": "dim_",
        "fact_prefix": "fact_",
        "timestamp_column": "ingestion_timestamp",
        "source_column": "source_system",
    }
    
    # Data Quality Standards
    QUALITY_STANDARDS = {
        "completeness_threshold": 0.99,  # 99% of records
        "accuracy_validation": True,
        "timeliness_sla_hours": 24,
        "deduplication_required": True,
    }
    
    # Access Control
    ACCESS_CONTROL = {
        "default_classification": DataClassification.INTERNAL.value,
        "mfa_required": True,
        "audit_logging": True,
        "data_retention_days": 2555,  # 7 years
    }
    
    # Monitoring & Alerting
    MONITORING = {
        "data_quality_check_frequency": "daily",
        "freshness_check_frequency": "hourly",
        "anomaly_detection": True,
        "alert_on_null_rate_above": 0.05,  # 5%
    }
    
    # Documentation Requirements
    DOCUMENTATION = {
        "schema_documentation_required": True,
        "lineage_tracking_required": True,
        "sla_documentation_required": True,
        "owner_assignment_required": True,
    }


@dataclass
class TableMetadata:
    """Metadata for data tables"""
    table_name: str
    layer: str  # bronze, silver, gold
    domain: str  # operations, finance, risk
    description: str
    owner: str
    classification: DataClassification
    created_date: datetime
    updated_date: datetime
    sla_hours: int
    quality_rules: List[DataQualityRule]
    upstream_tables: List[str]
    downstream_tables: List[str]


class GovernanceFramework:
    """Platform governance framework"""
    
    # Data Domain Definitions
    DOMAINS = {
        "operations": {
            "description": "Production, storage, distribution, and consumption data",
            "criticality": "critical",
            "tables": [
                "fact_production",
                "fact_delivery",
                "fact_consumption",
                "dim_plant",
                "dim_hospital",
                "dim_truck",
                "dim_time",
            ],
        },
        "finance": {
            "description": "Cost, margin, and financial analysis",
            "criticality": "high",
            "tables": [
                "fact_cost",
                "fact_margin",
                "dim_cost_center",
                "dim_customer",
            ],
        },
        "risk": {
            "description": "Dry-out risk and operational risk assessment",
            "criticality": "critical",
            "tables": [
                "fact_risk_score",
                "fact_autonomy_forecast",
                "dim_constraint",
            ],
        },
    }
    
    # Definition of Done (DoD)
    DEFINITION_OF_DONE = [
        "Code is committed and documented",
        "Data validation checks are implemented",
        "Architecture decisions are justified",
        "README/documentation is updated",
        "Pull Request is reviewed and approved",
        "Automated tests pass (unit + integration)",
        "Code quality checks pass (linting, type checking)",
        "Data quality SLAs are met",
        "Performance benchmarks are acceptable",
        "Security scanning shows no high/critical vulnerabilities",
    ]
    
    # Service Level Agreements (SLAs)
    SLAS = {
        "bronze_ingestion_lag": "< 1 hour",
        "silver_transformation_lag": "< 2 hours",
        "gold_refresh_frequency": "every 4 hours",
        "analytics_query_response": "< 5 seconds (p95)",
        "ai_agent_response": "< 10 seconds (p95)",
        "data_availability": "99.5%",
    }
    
    @staticmethod
    def validate_table_definition(metadata: TableMetadata) -> List[str]:
        """
        Validate table against governance policies
        
        Args:
            metadata: Table metadata
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Check naming conventions
        policy = DataGovernancePolicy.NAMING_CONVENTIONS
        expected_prefix = {
            "bronze": policy["table_prefix_bronze"],
            "silver": policy["table_prefix_silver"],
            "gold": policy["table_prefix_gold"],
        }
        
        if not metadata.table_name.startswith(expected_prefix.get(metadata.layer, "")):
            errors.append(
                f"Table name '{metadata.table_name}' does not follow naming convention for {metadata.layer} layer"
            )
        
        # Check required metadata
        if not metadata.owner:
            errors.append("Table owner must be assigned")
        
        if not metadata.quality_rules:
            errors.append("At least one quality rule must be defined")
        
        return errors


__all__ = [
    "DataGovernancePolicy",
    "TableMetadata",
    "GovernanceFramework",
    "DataClassification",
    "DataQualityRule",
]
