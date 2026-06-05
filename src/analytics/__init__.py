"""
Core Analytics Module for PI-1 Platform

Implements dry-out prevention and risk scoring analytics.
"""

from typing import Dict, List, Tuple
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lag,
    avg,
    max,
    min,
    Window,
    datediff,
    current_date,
)
from dataclasses import dataclass
from enum import Enum


class RiskLevel(Enum):
    """Risk level classification"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class DryOutRisk:
    """Dry-out risk assessment result"""
    hospital_id: str
    hospital_name: str
    current_autonomy_hours: float
    forecast_autonomy_hours: float
    risk_level: RiskLevel
    risk_score: float  # 0-100
    days_until_critical: float
    recommended_actions: List[str]
    alert_triggered: bool


class AutonomyForecast:
    """
    Hospital Tank Autonomy Forecasting
    
    Forecasts how long a hospital's LOX storage will last based on:
    - Current storage level
    - Historical consumption patterns
    - Seasonal/demand trends
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def calculate_current_autonomy(
        self,
        storage_level: float,
        daily_consumption: float,
        safety_margin_hours: float = 24,
    ) -> float:
        """
        Calculate current tank autonomy in hours
        
        Args:
            storage_level: Current LOX in units
            daily_consumption: Daily consumption in units
            safety_margin_hours: Hours to reserve for safety
        
        Returns:
            Autonomy in hours (excluding safety margin)
        """
        if daily_consumption <= 0:
            return float('inf')
        
        hourly_consumption = daily_consumption / 24
        autonomy_hours = (storage_level / hourly_consumption) if hourly_consumption > 0 else 0
        
        return max(0, autonomy_hours - safety_margin_hours)
    
    def forecast_consumption(
        self,
        consumption_df: DataFrame,
        hospital_id: str,
        forecast_days: int = 7,
    ) -> Dict[str, float]:
        """
        Forecast future consumption using historical averages
        
        Args:
            consumption_df: Historical consumption data
            hospital_id: Hospital identifier
            forecast_days: Number of days to forecast
        
        Returns:
            Daily consumption forecast for next N days
        """
        hospital_data = consumption_df.filter(col("hospital_id") == hospital_id)
        
        # Calculate average consumption
        avg_consumption = hospital_data.agg(
            {"daily_consumption": "avg"}
        ).collect()[0][0]
        
        # Simple forecast: repeat average
        forecast = {f"day_{i+1}": avg_consumption for i in range(forecast_days)}
        
        return forecast


class RiskScoringEngine:
    """
    Early-Warning Dry-Out Risk Scoring
    
    Assigns risk scores (0-100) based on:
    - Current and forecasted autonomy
    - Maintenance windows
    - Distribution constraints
    - Demand trends
    """
    
    CRITICAL_AUTONOMY_HOURS = 24  # Less than 1 day
    HIGH_AUTONOMY_HOURS = 72  # Less than 3 days
    MEDIUM_AUTONOMY_HOURS = 168  # Less than 7 days
    
    @staticmethod
    def score_hospital_risk(
        hospital_id: str,
        current_autonomy: float,
        forecasted_autonomy: float,
        maintenance_days_remaining: int = None,
        constraint_violations: int = 0,
    ) -> RiskLevel:
        """
        Calculate risk level for a hospital
        
        Args:
            hospital_id: Hospital identifier
            current_autonomy: Current autonomy in hours
            forecasted_autonomy: Forecasted autonomy in hours
            maintenance_days_remaining: Days until maintenance affects this hospital
            constraint_violations: Number of active constraint violations
        
        Returns:
            RiskLevel assessment
        """
        
        # Base risk on current autonomy
        if current_autonomy < RiskScoringEngine.CRITICAL_AUTONOMY_HOURS:
            return RiskLevel.CRITICAL
        elif current_autonomy < RiskScoringEngine.HIGH_AUTONOMY_HOURS:
            risk = RiskLevel.HIGH
        elif current_autonomy < RiskScoringEngine.MEDIUM_AUTONOMY_HOURS:
            risk = RiskLevel.MEDIUM
        else:
            risk = RiskLevel.LOW
        
        # Escalate if forecast is worse
        if forecasted_autonomy < current_autonomy:
            if risk == RiskLevel.LOW:
                risk = RiskLevel.MEDIUM
            elif risk == RiskLevel.MEDIUM:
                risk = RiskLevel.HIGH
            elif risk == RiskLevel.HIGH:
                risk = RiskLevel.CRITICAL
        
        # Escalate if maintenance is pending
        if maintenance_days_remaining and maintenance_days_remaining < 7:
            if risk == RiskLevel.LOW or risk == RiskLevel.MEDIUM:
                risk = RiskLevel.HIGH
        
        return risk
    
    @staticmethod
    def calculate_numerical_score(risk_level: RiskLevel) -> float:
        """Convert risk level to numerical score (0-100)"""
        scores = {
            RiskLevel.LOW: 25,
            RiskLevel.MEDIUM: 50,
            RiskLevel.HIGH: 75,
            RiskLevel.CRITICAL: 95,
        }
        return scores.get(risk_level, 0)


class CorrectiveActionRecommender:
    """
    Recommends corrective actions for high-risk situations:
    - Reroute: Send additional deliveries from alternative plants
    - Expedite: Speed up scheduled deliveries
    - Rebalance: Redistribute stock across network
    - Defer Maintenance: Postpone maintenance windows
    """
    
    @staticmethod
    def recommend_actions(risk: DryOutRisk) -> List[str]:
        """
        Generate list of recommended corrective actions
        
        Args:
            risk: DryOutRisk assessment
        
        Returns:
            List of recommended actions
        """
        actions = []
        
        if risk.risk_level == RiskLevel.CRITICAL:
            actions.append("URGENT: Send emergency delivery from nearest plant")
            actions.append("Activate redundant supply routes")
            actions.append("Alert hospital and patient care teams")
            actions.append("Defer any maintenance in production network")
            
        elif risk.risk_level == RiskLevel.HIGH:
            actions.append("Expedite next scheduled delivery")
            actions.append("Pre-position additional truck capacity")
            actions.append("Monitor consumption patterns closely")
            if risk.days_until_critical < 3:
                actions.append("Defer plant maintenance")
                
        elif risk.risk_level == RiskLevel.MEDIUM:
            actions.append("Review and optimize delivery schedule")
            actions.append("Monitor consumption trends")
            
        return actions


class DryOutPreventionAnalytics:
    """Main analytics module for dry-out prevention"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.autonomy = AutonomyForecast(spark)
        self.risk_scorer = RiskScoringEngine()
        self.recommender = CorrectiveActionRecommender()
    
    def assess_network_risk(
        self,
        hospitals_df: DataFrame,
        consumption_df: DataFrame,
        storage_df: DataFrame,
    ) -> List[DryOutRisk]:
        """
        Assess dry-out risk across the entire hospital network
        
        Args:
            hospitals_df: Hospital reference data
            consumption_df: Consumption data
            storage_df: Current storage levels
        
        Returns:
            List of risk assessments for each hospital
        """
        
        risks = []
        
        for hospital in hospitals_df.collect():
            hospital_id = hospital.hospital_id
            
            # Get current storage and consumption
            latest_consumption = consumption_df.filter(
                col("hospital_id") == hospital_id
            ).select("daily_consumption").limit(1)
            
            current_storage = storage_df.filter(
                col("hospital_id") == hospital_id
            ).select("storage_level_eod").limit(1)
            
            if latest_consumption.count() > 0 and current_storage.count() > 0:
                daily_consumption = latest_consumption.collect()[0][0]
                storage_level = current_storage.collect()[0][0]
                
                # Calculate autonomy
                autonomy = self.autonomy.calculate_current_autonomy(
                    storage_level, daily_consumption
                )
                
                # Forecast
                forecast = self.autonomy.forecast_consumption(
                    consumption_df, hospital_id
                )
                forecast_autonomy = min(forecast.values())
                
                # Score risk
                risk_level = self.risk_scorer.score_hospital_risk(
                    hospital_id, autonomy, forecast_autonomy
                )
                
                # Get recommendations
                risk = DryOutRisk(
                    hospital_id=hospital_id,
                    hospital_name=hospital.hospital_name,
                    current_autonomy_hours=autonomy,
                    forecast_autonomy_hours=forecast_autonomy,
                    risk_level=risk_level,
                    risk_score=self.risk_scorer.calculate_numerical_score(risk_level),
                    days_until_critical=(autonomy / 24) if autonomy > 0 else 0,
                    recommended_actions=self.recommender.recommend_actions(
                        DryOutRisk(
                            hospital_id, hospital.hospital_name,
                            autonomy, forecast_autonomy, risk_level,
                            self.risk_scorer.calculate_numerical_score(risk_level),
                            (autonomy / 24), [], risk_level == RiskLevel.CRITICAL
                        )
                    ),
                    alert_triggered=risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL],
                )
                
                risks.append(risk)
        
        return risks


__all__ = [
    "RiskLevel",
    "DryOutRisk",
    "AutonomyForecast",
    "RiskScoringEngine",
    "CorrectiveActionRecommender",
    "DryOutPreventionAnalytics",
]
