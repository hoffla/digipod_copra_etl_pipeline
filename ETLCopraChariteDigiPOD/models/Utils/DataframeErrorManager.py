import os
from typing import Optional
import pandas as pd
from dataclasses import dataclass, field

from pandas import DataFrame

from models.Utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class DataframeErrorManager:
    storage_file: str = field(init=False)
    error_dataframe: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())

    def __post_init__(self):
        self.storage_file = os.path.join(os.getenv('BASEPATH'), os.getenv('PROJECTDIRECTORY'), os.getenv('LOCAL_STORAGE'), 'errorDataframe.parquet')
        if os.path.exists(self.storage_file):
            self._load_from_disk()
        else:
            logger.debug("No existing data found on disk. Starting with an empty DataFrame.")

    @staticmethod
    def reset_lower_index_for_group(df, group_name, level='tableName'):
        original_index = df.index
        new_index = []
        new_idx = 0
        for (class_value, obj_value, row_index) in original_index:
            if class_value == level and obj_value == group_name:
                new_index.append((class_value,  obj_value, new_idx))
                new_idx += 1
            else:
                new_index.append((class_value,  obj_value, row_index))

        df.index = pd.MultiIndex.from_tuples(new_index, names=df.index.names)

        return df

    @staticmethod
    def _add_multi_index(tableName: str, deserializedObj: str, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            logger.warning(f"DataFrame for table '{tableName}' and object '{deserializedObj}' is empty.")
            return df

        multi_index = pd.MultiIndex.from_product([[tableName], [deserializedObj], df.index], names=["tableName", "deserializedObj", "row_index"])
        df.index = multi_index
        return df

    def _save_to_disk(self) -> None:
        try:
            self.error_dataframe.to_parquet(self.storage_file)
            logger.info(f"DataFrame saved to disk at '{self.storage_file}'.")
        except Exception as e:
            logger.error(f"Failed to save DataFrame to disk: {e}")

    def _load_from_disk(self) -> None:
        try:
            self.error_dataframe = pd.read_parquet(self.storage_file)
            logger.info(f"DataFrame loaded from disk at '{self.storage_file}'.")
        except Exception as e:
            logger.error(f"Failed to load DataFrame from disk: {e}")

    def save(self, tableName: str, df_dict: dict[str, DataFrame]) -> None:
        for deserializedObj, df in df_dict.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                logger.warning(f"No valid DataFrame to save for deserialized object '{deserializedObj}' in table '{tableName}'.")
                continue

            df_with_index = self._add_multi_index(tableName, deserializedObj, df)

            if not self.error_dataframe.empty:
                self.error_dataframe = pd.concat([self.error_dataframe, df_with_index], ignore_index=False)
                logger.info(f"Concatenated new rows for table '{tableName}' and object '{deserializedObj}'.")
            else:
                self.error_dataframe = df_with_index
                logger.info(f"Saved DataFrame for table '{tableName}' and object '{deserializedObj}'.")

        self.error_dataframe = self.error_dataframe.drop_duplicates()
        self._save_to_disk()

    def load(self, tableName: str) -> Optional[dict]:
        if self.error_dataframe.empty:
            logger.info(f"No error DataFrame available.")
            return None

        if tableName in self.error_dataframe.index.get_level_values('tableName'):
            filtered_df = self.error_dataframe.xs(tableName, level='tableName')
            deserialized_objects = filtered_df.index.get_level_values('deserializedObj').unique()

            result_dict = {obj: filtered_df.xs(obj, level='deserializedObj') for obj in deserialized_objects}
            logger.info(f"Loaded error DataFrame for table '{tableName}'.")
            return result_dict
        else:
            logger.info(f"No error DataFrame found for table '{tableName}'.")
            return None

    def delete(self, tableName: str) -> None:
        if self.error_dataframe.empty:
            logger.warning(f"No DataFrame found to delete for table '{tableName}'.")
            return

        if tableName in self.error_dataframe.index.get_level_values('tableName'):
            self.error_dataframe = self.error_dataframe.drop(tableName, level='tableName')
            logger.info(f"Deleted DataFrame for table '{tableName}'.")

            self._save_to_disk()
        else:
            logger.warning(f"No DataFrame found for table '{tableName}' to delete.")

    def list_tables(self) -> list:
        if not self.error_dataframe.empty:
            return list(self.error_dataframe.index.get_level_values('tableName').unique())
        return []

    def clear_all(self) -> None:
        self.error_dataframe = pd.DataFrame()
        self._save_to_disk()
        logger.info("Cleared all DataFrames from memory and disk.")
