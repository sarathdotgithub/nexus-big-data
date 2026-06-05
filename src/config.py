"""
Configuration Management for PI-1 Platform
"""

import os
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv


@dataclass
class SparkConfig:
    """Spark cluster configuration"""
    master_host: str = "spark-master"
    master_port: int = 7077
    worker_cores: int = 2
    worker_memory: str = "2G"
    
    @property
    def master_url(self) -> str:
        return f"spark://{self.master_host}:{self.master_port}"


@dataclass
class DataLakeConfig:
    """Data Lake storage configuration"""
    bronze_path: str = "/workspace/data/bronze"
    silver_path: str = "/workspace/data/silver"
    gold_path: str = "/workspace/data/gold"
    raw_path: str = "/workspace/data/raw"
    
    def create_directories(self) -> None:
        """Ensure all data lake directories exist"""
        for path in [self.bronze_path, self.silver_path, self.gold_path, self.raw_path]:
            Path(path).mkdir(parents=True, exist_ok=True)


@dataclass
class APIConfig:
    """API Server configuration"""
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    workers: int = 4


@dataclass
class AIConfig:
    """AI/LLM configuration"""
    openai_api_key: Optional[str] = None
    model: str = "gpt-4"
    temperature: float = 0.7
    max_tokens: int = 2000


class PlatformConfig:
    """Main platform configuration"""
    
    def __init__(self, env_file: Optional[str] = None):
        """
        Initialize platform configuration from environment
        
        Args:
            env_file: Path to .env file (default: config/.env)
        """
        if env_file is None:
            env_file = "config/.env"
        
        # Load environment variables
        if os.path.exists(env_file):
            load_dotenv(env_file)
        else:
            load_dotenv()
        
        # Initialize sub-configurations
        self.spark = SparkConfig(
            master_host=os.getenv("SPARK_MASTER_HOST", "spark-master"),
            master_port=int(os.getenv("SPARK_MASTER_PORT", 7077)),
            worker_cores=int(os.getenv("SPARK_WORKER_CORES", 2)),
            worker_memory=os.getenv("SPARK_WORKER_MEMORY", "2G"),
        )
        
        self.data_lake = DataLakeConfig(
            bronze_path=os.getenv("BRONZE_PATH", "/workspace/data/bronze"),
            silver_path=os.getenv("SILVER_PATH", "/workspace/data/silver"),
            gold_path=os.getenv("GOLD_PATH", "/workspace/data/gold"),
            raw_path=os.getenv("RAW_PATH", "/workspace/data/raw"),
        )
        
        self.api = APIConfig(
            host=os.getenv("API_HOST", "0.0.0.0"),
            port=int(os.getenv("API_PORT", 8000)),
            debug=os.getenv("DEBUG", "false").lower() == "true",
            workers=int(os.getenv("API_WORKERS", 4)),
        )
        
        self.ai = AIConfig(
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            model=os.getenv("LLM_MODEL", "gpt-4"),
            temperature=float(os.getenv("LLM_TEMPERATURE", 0.7)),
            max_tokens=int(os.getenv("LLM_MAX_TOKENS", 2000)),
        )
    
    def initialize(self) -> None:
        """Initialize all platform resources"""
        self.data_lake.create_directories()
    
    def to_dict(self) -> dict:
        """Export configuration as dictionary"""
        return {
            "spark": {
                "master_url": self.spark.master_url,
                "worker_cores": self.spark.worker_cores,
                "worker_memory": self.spark.worker_memory,
            },
            "data_lake": {
                "bronze": self.data_lake.bronze_path,
                "silver": self.data_lake.silver_path,
                "gold": self.data_lake.gold_path,
                "raw": self.data_lake.raw_path,
            },
            "api": {
                "host": self.api.host,
                "port": self.api.port,
                "debug": self.api.debug,
            },
        }
