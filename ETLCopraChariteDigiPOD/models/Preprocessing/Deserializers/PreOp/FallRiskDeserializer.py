from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.PreOp.FallRisk import FallRisk
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper


@dataclass
class FallRiskDeserializer(BaseDeserializer):
    def deserialize(self) -> FallRisk:
        gestuerzt = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN445'))
        anzahl = self._parse_int_value('QVDELIN446')
        nicht_erhoben = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN447'))

        return FallRisk(
            gestuerzt=gestuerzt,
            anzahl=anzahl,
            nicht_erhoben=nicht_erhoben
        )

    def _parse_int_value(self, path: str) -> Optional[int]:
        value = self._get_element_value(path)
        try:
            return int(value) if value else None
        except ValueError:
            return None
