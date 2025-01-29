from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.PreOp.MiniCog import MiniCog
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper


@dataclass
class MiniCogDeserializer(BaseDeserializer):
    def deserialize(self) -> MiniCog:
        result = self._parse_int_value('QVDELIN029')
        word_score = self._parse_int_value('QVDELIN338')

        clock_score = self._map_value('QVDELIN339', {
            "0": "Abnormale Uhr",
            "2": "Normale Uhr",
            "": "k.A."
        })

        not_recorded = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN340'))
        reason_not_recorded = self._get_element_value('QVDELIN341')

        word_version = self._map_value('QVDELIN448', {
            "v1": "Version 1",
            "v2": "Version 2",
            "": "k.A."
        })

        return MiniCog(
            result=result,
            word_score=word_score,
            clock_score=clock_score,
            not_recorded=not_recorded,
            reason_not_recorded=reason_not_recorded,
            word_version=word_version
        )

    def _parse_int_value(self, path: str) -> Optional[int]:
        value = self._get_element_value(path)
        if value and value.isdigit():
            return int(value)
        return None

    def _map_value(self, path: str, mapping: dict) -> Optional[str]:
        value = self._get_element_value(path)
        return mapping.get(value.strip().lower()) if value else None
