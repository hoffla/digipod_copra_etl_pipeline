from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.PreOp.Nicotin import NicotinConsumptionScore
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper


@dataclass
class NikotinkonsumScoreDeserializer(BaseDeserializer):
    def deserialize(self) -> NicotinConsumptionScore:
        total_score = self._parse_int_value('QVDELIN378')

        question1 = self._map_question1_value('QVDELIN409')
        question1_years = self._parse_int_value('QVDELIN410')

        question2 = self._map_question2_value('QVDELIN411')
        question2_amount = self._parse_int_value('QVDELIN412')

        question3 = self._parse_int_value('QVDELIN413')
        question4 = self._map_question4_value('QVDELIN414')

        not_collected = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN415'))

        return NicotinConsumptionScore(
            total_score=total_score,
            question1=question1,
            question1_years=question1_years,
            question2=question2,
            question2_amount=question2_amount,
            question3=question3,
            question4=question4,
            not_collected=not_collected
        )

    def _parse_int_value(self, path: str) -> Optional[int]:
        value = self._get_element_value(path)
        if value and value.isdigit():
            return int(value)
        return None

    def _map_question1_value(self, path: str) -> Optional[str]:
        value = self._get_element_value(path)
        return {
            "n": "Nein",
            "nf": "Nein, aber früher",
            "j": "Ja, aktiv"
        }.get(value.strip().lower()) if value else None

    def _map_question2_value(self, path: str) -> Optional[str]:
        value = self._get_element_value(path)
        return {
            "0": "0-10",
            "1": "11-20",
            "2": "21-30",
            "3": "=> 31",
            "": "k.A."
        }.get(value.strip().lower()) if value else None

    def _map_question4_value(self, path: str) -> Optional[str]:
        value = self._get_element_value(path)
        return {
            "0": "Mehr als 60 Minuten",
            "1": "31-60 Minuten",
            "2": "6-30 Minuten",
            "3": "Innerhalb 5 Minuten",
            "": "k.A."
        }.get(value.strip().lower()) if value else None
