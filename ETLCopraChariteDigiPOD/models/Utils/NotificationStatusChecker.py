import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from models.Utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class NotificationStatusChecker:
    notification_file: Path = field(init=False)

    def __post_init__(self):
        basePath, projectPath, ressourcePath = os.getenv('BASEPATH'), os.getenv('PROJECTDIRECTORY'), os.getenv('RESSOURCES_DIR')
        self.notification_file = Path(basePath, projectPath, ressourcePath, 'notification_status.json')

    def _read_notification_file(self) -> dict:
        if self.notification_file.exists():
            try:
                with open(self.notification_file, 'r') as file:
                    data = json.load(file)
                logger.info(f"Notification file read successfully: {data}")
                return data
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON file: {e}")
        else:
            logger.error(f"Notification file '{self.notification_file}' does not exist.")
        return {}

    def _write_notification_file(self, data: dict) -> None:
        try:
            with open(self.notification_file, 'w') as file:
                json.dump(data, file, indent=4)
            logger.info(f"Notification file updated successfully: {data}")
        except IOError as e:
            logger.error(f"Failed to write to notification file: {e}")

    def check_and_update_status(self) -> bool:
        """
        Verifica o status da notificação no arquivo JSON. Se for False, muda para True.
        Retorna True se no arquivo estiver False, e False se estiver True.
        """
        data = self._read_notification_file()
        notification_processed = data.get("notification_processed")

        if isinstance(notification_processed, bool):
            if not notification_processed:
                data["notification_processed"] = True
                self._write_notification_file(data)
                logger.info("New notification status was found in the file. Starting processing of quarantine data.")
                return True
        return False
