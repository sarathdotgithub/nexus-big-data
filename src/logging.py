"""
Logging Configuration for PI-1 Platform
"""

import sys
import logging
from loguru import logger as loguru_logger


def setup_logging(module_name: str, level: str = "INFO") -> logging.Logger:
    """
    Configure structured logging for the platform
    
    Args:
        module_name: Name of the module for log identification
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Configured logger instance
    """
    
    # Remove default handler
    loguru_logger.remove()
    
    # Add console handler with formatting
    loguru_logger.add(
        sys.stdout,
        format="<level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=level,
    )
    
    # Add file handler for persistent logging
    loguru_logger.add(
        "logs/pi1-platform.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level=level,
        rotation="500 MB",
        retention="10 days",
    )
    
    return loguru_logger.bind(module=module_name)


# Create default logger
logger = setup_logging(__name__)

__all__ = ["setup_logging", "logger"]
