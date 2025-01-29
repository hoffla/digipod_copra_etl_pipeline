import json
import os
from pathlib import Path

import requests

from models.Utils.logger import get_logger

logger = get_logger(__name__)


def singleton(cls):
    instances = {}

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance


@singleton
class ExceptionsRaisedDetector:
    def __init__(self):
        self.exceptions_raised = {}
        self.storage_file = Path(os.getenv('BASEPATH'), os.getenv('PROJECTDIRECTORY'), os.getenv('RESSOURCES_DIR'), 'exceptions_raised.json')
        self._load_from_disk()

    def set_exception_status(self, table_name: str, status: bool) -> None:
        if status:
            self.exceptions_raised['newException'] = True
        self.exceptions_raised[table_name] = status
        self._save_to_disk()

    def get_exception_status(self, table_name: str) -> bool:
        return self.exceptions_raised.get(table_name, False)

    def reset_all_status(self) -> None:
        self.exceptions_raised = {}
        self._save_to_disk()

    def _save_to_disk(self) -> None:
        try:
            with open(self.storage_file, 'w') as file:
                json.dump(self.exceptions_raised, file, indent=4)
            logger.debug(f"Exceptions saved to disk at {self.storage_file}")
        except IOError as e:
            logger.error(f"Error saving exceptions to disk: {e}")

    def _load_from_disk(self) -> None:
        if self.storage_file.exists():
            try:
                with open(self.storage_file, 'r') as file:
                    self.exceptions_raised = json.load(file)
                logger.debug(f"Exceptions loaded from {self.storage_file}")
            except (IOError, ValueError) as e:
                logger.error(f"Error loading exceptions from disk: {e}")
        else:
            logger.warning(f"No existing exceptions file found. Starting with an empty state.")

    
@singleton
class TableUpdateStatusDetector:
    def __init__(self):
        self.updates = {}
        self.informationEndpoint = os.getenv('EXECUTION_ENGINE_ENDPOINT')
    
    def set_update_status(self, table_name: str, status: bool) -> None:
        self.updates[table_name] = status
        
    def get_update_status(self, table_name: str) -> bool:
        return self.updates.get(table_name, False)
    
    def inform_if_updates(self) -> None:
        try:
            for updateStatus in self.updates.values():
                if updateStatus:
                    requests.post(f'http://{self.informationEndpoint}', data={'message': 'update'})
                    logger.info("Execute engine was informed of new updated.")
                    break

            self.reset_all_status()
        except Exception as err:
            logger.error(f"Error during processing of update information request!. Error: {err}")
    
    def reset_all_status(self) -> None:
        self.updates = {}
