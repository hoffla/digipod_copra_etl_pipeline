from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.PreOp.Frailty import FrailtyScore
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper

import decimal


@dataclass
class FrailtyScoreDeserializer(BaseDeserializer):
    def deserialize(self) -> FrailtyScore:
        total_pathological_criteria = self._parse_int_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN030')
        
        criterion_1 = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN429'))
        criterion_1_weight = self._parse_decimal_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN430')
        criterion_2 = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN431'))
        criterion_2_frequency = self._map_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN432'), {
            "1": "selten (<1 Tag/Woche)", "2": "gelegentlich (1-2 Tage/Woche)", "3": "häufig (3-4 Tage/Woche)",
            "4": "die meiste Zeit (5-7 Tage/Woche)", "": "k.A."
        })
        criterion_3 = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN433'))
        criterion_3_metabolism = self._map_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN434'), {
            "1": "Ruhig liegen, Sitzen, Toilette benutzen", "2": "Umhergehen, allein anziehen",
            "3": "In der ebenen Strecke spazieren gehen", "4": "Leichte Hausarbeit, Treppensteigen = 2 Etagen",
            "5": "Treppensteigen > 2 Etagen", "6": "Radfahren langsam, Gartenarbeit", "7": "Schwimmen, Joggen",
            "8": "Radfahren schnell, Bergwandern", "9": "Joggen ausdauernd", "10": "Tennis, Ballsport",
            "11": "Leistungssport", "": "k.A."
        })
        criterion_4 = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN435'))
        criterion_4_hand_strength_1 = self._parse_decimal_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN436')
        criterion_4_hand_strength_2 = self._parse_decimal_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN437')
        criterion_4_hand_strength_3 = self._parse_decimal_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN438')
        criterion_4_hand_strength_not_possible = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN439'))
        criterion_5 = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN440'))
        criterion_5_seconds = self._parse_decimal_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN441')
        criterion_5_not_possible = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN442'))
        not_collected = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN443'))

        return FrailtyScore(
            total_pathological_criteria=total_pathological_criteria,
            criterion_1=criterion_1,
            criterion_1_weight=criterion_1_weight,
            criterion_2=criterion_2,
            criterion_2_frequency=criterion_2_frequency,
            criterion_3=criterion_3,
            criterion_3_metabolism=criterion_3_metabolism,
            criterion_4=criterion_4,
            criterion_4_hand_strength_1=criterion_4_hand_strength_1,
            criterion_4_hand_strength_2=criterion_4_hand_strength_2,
            criterion_4_hand_strength_3=criterion_4_hand_strength_3,
            criterion_4_hand_strength_not_possible=criterion_4_hand_strength_not_possible,
            criterion_5=criterion_5,
            criterion_5_seconds=criterion_5_seconds,
            criterion_5_not_possible=criterion_5_not_possible,
            not_collected=not_collected
        )

    def _parse_int_value(self, path: str) -> Optional[int]:
        value = self._get_element_value(path)
        try:
            return int(value) if value else None
        except ValueError:
            return None

    def _parse_decimal_value(self, path: str) -> Optional[decimal.Decimal]:
        value = self._get_element_value(path)
        try:
            return decimal.Decimal(value) if value else None
        except decimal.InvalidOperation:
            return None

    def _map_value(self, value: Optional[str], mapping: dict) -> Optional[str]:
        return mapping.get(value.strip().lower()) if value else None
