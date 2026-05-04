"""Logging configuration for data pipeline."""
import sys
from loguru import logger

# Remove default handler
logger.remove()

# Add console handler with better formatting
logger.add(
    sys.stderr,
    format="<level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO",
)

# Add file handler for production
logger.add(
    "logs/pipeline.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    level="DEBUG",
    rotation="500 MB",
)
