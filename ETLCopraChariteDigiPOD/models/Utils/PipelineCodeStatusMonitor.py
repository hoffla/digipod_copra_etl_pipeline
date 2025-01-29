import json
import os
import hashlib
from pathlib import Path
from typing import Dict, Tuple
from dataclasses import dataclass, field
from datetime import datetime

from models.Utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class PipelineCodeStatusMonitor:
    directory: Path = field(init=False)
    state_file: Path = field(init=False)
    file_info: Dict[str, Tuple[str, datetime]] = field(default_factory=dict)  # Armazena hash e timestamp

    def __post_init__(self):
        directory = os.path.join(os.getenv('BASEPATH'), os.getenv('PROJECTDIRECTORY'), os.getenv('OMOP_PIPELINES_DIR'))
        state_file = os.path.join(os.getenv('BASEPATH'), os.getenv('PROJECTDIRECTORY'), os.getenv('RESSOURCES_DIR'), 'state.json')
        self.directory = Path(directory)
        self.state_file = Path(state_file)

        if not self.directory.is_dir():
            raise ValueError(f"{self.directory} is not a valid directory.")

        self._load_state()
        self._initialize_file_info()

    def _load_state(self):
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as file:
                    file_info_json = json.load(file)
                    self.file_info = {
                        file: (hash_value, datetime.fromisoformat(timestamp))
                        for file, (hash_value, timestamp) in file_info_json.items()
                    }
                    logger.info(f"File state loaded from {self.state_file}.")
            except (IOError, ValueError, KeyError) as e:
                logger.error(f"Error loading state from {self.state_file}: {e}")
        else:
            logger.info(f"State file {self.state_file} not found. It will be created shortly.")

    def _save_state(self):
        try:
            with open(self.state_file, 'w') as file:
                file_info_json = {
                    file: (hash_value, timestamp.isoformat())
                    for file, (hash_value, timestamp) in self.file_info.items()
                }
                json.dump(file_info_json, file, indent=4)
                logger.info(f"File state saved to {self.state_file}.")
        except IOError as e:
            logger.error(f"Error saving state to {self.state_file}: {e}")

    def _initialize_file_info(self):
        files_found = False
        for file in self._python_files():
            if file not in self.file_info:  # Processa apenas arquivos que ainda não foram verificados
                self.file_info[file] = (self._calculate_hash(file), self._get_last_modified_time(file))
                files_found = True

        if files_found:
            self._save_state()
        else:
            logger.info("No new files found to initialize in the state.")

    def _python_files(self):
        return [str(f) for f in self.directory.glob("*.py") if f.is_file()]

    @staticmethod
    def _calculate_hash(file_path: str) -> str:
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as file:
            buffer = file.read()
            hasher.update(buffer)
        return hasher.hexdigest()

    @staticmethod
    def _get_last_modified_time(file_path: str) -> datetime:
        return datetime.fromtimestamp(os.path.getmtime(file_path))

    def _file_has_changed(self, file_path: str) -> bool:
        current_hash = self._calculate_hash(file_path)
        current_timestamp = self._get_last_modified_time(file_path)
        previous_hash, previous_timestamp = self.file_info.get(file_path, (None, None))

        if current_hash != previous_hash or current_timestamp != previous_timestamp:
            self.file_info[file_path] = (current_hash, current_timestamp)
            self._save_state()
            return True
        return False

    def check_changes_file_for_table(self, table_name: str) -> bool:
        file_name = f"{self.to_camel_case(table_name)}Pipeline.py"
        file_path = self.directory / file_name

        if not file_path.exists():
            logger.warning(f"File {file_name} not found.")
            return False

        if self._file_has_changed(str(file_path)):
            logger.info(f"Changes detected in file: {file_name}")
            return True
        else:
            logger.info(f"No changes detected in file: {file_name}")
            return False

    @staticmethod
    def to_camel_case(snake_str_tableName):
        components = snake_str_tableName.split('_')
        camel_case_str = ''.join(x.capitalize() for x in components)

        return camel_case_str