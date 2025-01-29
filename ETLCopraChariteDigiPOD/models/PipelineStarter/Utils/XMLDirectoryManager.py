import os
import shutil

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import List
from collections.abc import Iterable

from models.Utils.logger import get_logger

logger = get_logger(__name__)
quarantineLogger = get_logger('quarantine_logger')


@dataclass
class XMLDirectoryManager:
    unprocessed_directory: Path
    processed_directory: Path
    error_deser_directory: Path
    error_dfProcessing_directory: Path
    quarantine_directory: Path

    lastCleansing: datetime = field(init=False, default_factory=datetime.now)

    def __get_directory(self, directory: str):
        directory_dir = {
            "unprocessed": self.unprocessed_directory,
            "processed": self.processed_directory,
            "error_deserialization": self.error_deser_directory,
            "error_df_processing": self.error_dfProcessing_directory,
            "quarantine": self.quarantine_directory
        }.get(directory)

        directory_dir.mkdir(parents=True, exist_ok=True)
        return directory_dir

    def get_list_files(self, directory_dir="unprocessed", include_processed=False) -> List[Path]:
        new_files = list(self.__get_directory(directory_dir).glob("*.xml"))
        if include_processed:
            old_files = list(self.__get_directory("processed").glob("*.xml"))
            all_files = new_files + old_files
            return all_files
        return new_files

    def mark_as(self, xml_paths: Iterable, directory: str) -> None:
        if isinstance(xml_paths, Iterable) and not isinstance(xml_paths, (str, bytes)):
            for xml_path in xml_paths:
                self._move_file(xml_path, directory)
        elif xml_paths and isinstance(xml_paths, (str, bytes)):
            self._move_file(xml_paths, directory)

    def _move_file(self, xml_path, destination_type: str):
        xml_filename = Path(xml_path).name if not isinstance(xml_path, Path) else xml_path.name

        destination_dir = self.__get_directory(destination_type)

        if not destination_dir or not destination_dir.exists():
            logger.warning(
                f"Destination directory '{destination_type}' is invalid or does not exist. File '{xml_path}' not moved.")
            return

        destination_path = destination_dir / xml_filename
        try:
            shutil.move(xml_path, destination_path)
            logger.info(f"File named '{xml_filename}' moved to {destination_type} directory: {destination_path}")
            if destination_type == "quarantine":
                quarantineLogger.info(f'File added: {os.path.basename(xml_path)}')
        except Exception as e:
            logger.error(f"Failed to move file {xml_filename} to {destination_type} directory. Error: {e}")

    def cleanOldQuarantineFiles(self, threshold=60):
        timeDiff = datetime.now() - self.lastCleansing
        if timeDiff.total_seconds() > 86400:  # Every 24h
            logger.info('Cleaning old quarantine files.')
            threshold_date = datetime.now() - timedelta(days=threshold)

            for filename in os.listdir(self.quarantine_directory):
                file_path = os.path.join(self.quarantine_directory, filename)

                if os.path.isfile(file_path):
                    file_mod_time = datetime.fromtimestamp(os.path.getctime(file_path))

                    if file_mod_time < threshold_date:
                        try:
                            os.remove(file_path)
                            quarantineLogger.info(f'Old file (older than {threshold} days) deleted: {filename}')
                        except Exception as err:
                            quarantineLogger.info(f'Error during deletion of file: {filename}. Error: {err}')

    def delete_files(self, file_paths: List[Path]) -> None:
        for file_path in file_paths:
            try:
                if file_path.exists() and file_path.is_file():
                    file_path.unlink()
                    logger.info(f"Successfully deleted file: {file_path}")
                else:
                    logger.warning(f"File does not exist or is not a valid file: {file_path}")
            except Exception as err:
                logger.error(f"Failed to delete file {file_path}. Error: {err}")
