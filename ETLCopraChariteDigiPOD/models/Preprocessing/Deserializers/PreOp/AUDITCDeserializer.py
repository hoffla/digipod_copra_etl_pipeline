from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.PreOp.AUDITC import AlkoholkonsumScore
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper

basePath = './/SUB_DOC/SUB_DOC_CONTENT/'


@dataclass
class AlkoholkonsumScoreDeserializer(BaseDeserializer):
    def deserialize(self) -> AlkoholkonsumScore:
        total_score = self._parse_int_value(basePath + 'QVDELIN392')

        question_1 = self._map_value(self._get_element_value(basePath + 'QVDELIN426'), {
            "0": "Nie", "1": "etwa 1 mal pro Monat", "2": "2-4 mal pro Monat", "3": "2-3 mal pro Woche",
            "4": "4 mal oder häufiger pro Woche", "": "k.A."
        })

        question_2 = self._map_value(self._get_element_value(basePath + 'QVDELIN427'), {
            "0": "1 oder 2", "1": "3 oder 4", "2": "5 oder 6", "3": "7 bis 9", "4": "10 oder mehr", "": "k.A."
        })

        question_3 = self._map_value(self._get_element_value(basePath + 'QVDELIN428'), {
            "0": "Nie", "1": "seltener als 1 mal pro Monat", "2": "1 mal pro Monat", "3": "1 mal pro Woche",
            "4": "täglich oder fast täglich", "": "k.A."
        })

        not_collected = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN444'))

        years_not_drinking = self._parse_int_value(basePath + 'QVDELIN449')

        return AlkoholkonsumScore(
            total_score=total_score,
            question_1=question_1,
            question_2=question_2,
            question_3=question_3,
            not_collected=not_collected,
            years_not_drinking=years_not_drinking
        )

    def _parse_int_value(self, path: str) -> Optional[int]:
        value = self._get_element_value(path)
        try:
            return int(value) if value else None
        except ValueError:
            return None

    @staticmethod
    def _map_value(value: Optional[str], mapping: dict) -> Optional[str]:
        return mapping.get(value.strip().lower()) if value else None
