from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.PreOp.Polypharmacy import PolypharmacyScore
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper


@dataclass
class PolypharmazieScoreDeserializer(BaseDeserializer):
    def deserialize(self) -> PolypharmacyScore:
        score = self._parse_long_value('QVDELIN034')
        not_collected = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN416'))

        return PolypharmacyScore(
            score=score,
            not_collected=not_collected
        )

    def _parse_long_value(self, path: str) -> Optional[int]:
        value = self._get_element_value(path)
        if value and value.isdigit():
            return int(value)
        return None
