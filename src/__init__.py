"""
PI-1 Platform Core Module

ManOxCo Oxygen Reliability Transformation - Data Platform
Version: 0.1.0
"""

__version__ = "0.1.0"
__author__ = "Nexus Consulting"

from .config import PlatformConfig
from .logging import setup_logging

# Initialize platform logging
logger = setup_logging(__name__)

__all__ = [
    "PlatformConfig",
    "setup_logging",
    "logger",
]
