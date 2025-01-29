import os
from dataclasses import dataclass, field

import pandas as pd

from models.Utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class DataFrameSaver:
    base_path: str = field(default_factory=lambda: os.getenv('BASEPATH'))
    project_directory: str = field(default_factory=lambda: os.getenv('PROJECTDIRECTORY'))
    storage_directory: str = field(default_factory=lambda: os.getenv('LOCAL_STORAGE'))

    def _generate_file_path(self, table_name: str, _format: str, file_path: str = None) -> str:
        if not file_path:
            file_path = os.path.join(self.base_path, self.project_directory, self.storage_directory)
        return os.path.join(file_path, f"{table_name}.{_format}")

    @staticmethod
    def _save_to_format(df: pd.DataFrame, file_path: str, _format: str, **kwargs) -> None:
        save_methods = {
            'csv': df.to_csv,
            'parquet': df.to_parquet,
            'xlsx': df.to_excel
        }

        save_method = save_methods.get(_format)
        if save_method:
            save_method(file_path, index=False, **kwargs)
            logger.info(f"Table '{os.path.basename(file_path)}' saved successfully in {_format} format.")
        else:
            logger.error(f"Unsupported format: {_format}")

    def save(self, table_name: str, df: pd.DataFrame, _format: str = 'csv', file_path: str = None, **kwargs) -> None:
        if not isinstance(df, pd.DataFrame) or df.empty:
            logger.info(f"The table '{table_name}' is empty or not a valid DataFrame, no file saved.")
            return

        try:
            file_path = self._generate_file_path(table_name, _format, file_path)
            self._save_to_format(df, file_path, _format, **kwargs)
        except Exception as err:
            logger.error(f"Failed to save table '{table_name}'. Error: {err}")
