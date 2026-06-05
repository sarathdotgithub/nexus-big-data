"""
Spark Session Manager for PI-1 Platform
"""

from typing import Optional
from pyspark.sql import SparkSession
from src.config import PlatformConfig
from src.logging import setup_logging

logger = setup_logging(__name__)


class SparkSessionManager:
    """Manages Spark session lifecycle and configuration"""
    
    _instance: Optional[SparkSession] = None
    
    @classmethod
    def get_or_create(
        cls,
        config: Optional[PlatformConfig] = None,
        app_name: str = "PI-1-ManOxCo",
    ) -> SparkSession:
        """
        Get or create Spark session
        
        Args:
            config: Platform configuration
            app_name: Spark application name
        
        Returns:
            SparkSession instance
        """
        if cls._instance is not None:
            return cls._instance
        
        if config is None:
            config = PlatformConfig()
        
        logger.info(f"Creating Spark session: {app_name}")
        logger.info(f"Master: {config.spark.master_url}")
        
        builder = (
            SparkSession.builder
            .appName(app_name)
            .master(config.spark.master_url)
            .config("spark.sql.shuffle.partitions", 10)
            .config("spark.default.parallelism", 10)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        )
        
        cls._instance = builder.getOrCreate()
        logger.info("Spark session created successfully")
        
        return cls._instance
    
    @classmethod
    def stop(cls) -> None:
        """Stop the Spark session"""
        if cls._instance is not None:
            logger.info("Stopping Spark session")
            cls._instance.stop()
            cls._instance = None


def get_spark_session(
    config: Optional[PlatformConfig] = None,
) -> SparkSession:
    """
    Convenience function to get Spark session
    
    Args:
        config: Platform configuration
    
    Returns:
        SparkSession instance
    """
    return SparkSessionManager.get_or_create(config)


__all__ = ["SparkSessionManager", "get_spark_session"]
