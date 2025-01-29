from dataclasses import dataclass

import psutil
from models.Utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class MemoryMonitor:
    threshold: float = 2.0

    @staticmethod
    def get_available_memory() -> float:
        memory_info = psutil.virtual_memory()
        available_memory_gb = memory_info.available / (1024 ** 3)
        return available_memory_gb

    def is_memory_critical(self) -> bool:
        available_memory = self.get_available_memory()
        if available_memory < self.threshold:
            logger.warning(f"Memory critical: {available_memory:.2f} GB available, below threshold of {self.threshold} GB.")
            return True
        logger.info(f"Memory sufficient: {available_memory:.2f} GB available.")
        return False

    def log_memory_usage(self):
        available_memory = self.get_available_memory()
        logger.info(f"Current memory usage: {available_memory:.2f} GB available.")