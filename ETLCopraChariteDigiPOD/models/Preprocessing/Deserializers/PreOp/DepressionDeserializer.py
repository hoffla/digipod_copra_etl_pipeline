from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.PreOp.Depression import DepressionScore
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper


@dataclass
class DepressionScoreDeserializer(BaseDeserializer):
    def deserialize(self) -> DepressionScore:
        score = self._parse_int_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN036')
        not_collected = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN408'))

        question_a = self._map_question_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN459')
        question_b = self._map_question_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN460')
        question_c = self._map_question_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN461')
        question_d = self._map_question_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN462')
        question_e = self._map_question_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN463')
        question_f = self._map_question_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN464')
        question_g = self._map_question_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN465')
        question_h = self._map_question_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN466')

        difficulty = self._map_difficulty_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN467')

        return DepressionScore(
            score=score,
            not_collected=not_collected,
            question_a=question_a,
            question_b=question_b,
            question_c=question_c,
            question_d=question_d,
            question_e=question_e,
            question_f=question_f,
            question_g=question_g,
            question_h=question_h,
            difficulty=difficulty
        )

    def _parse_int_value(self, path: str) -> Optional[int]:
        value = self._get_element_value(path)
        if value and value.isdigit():
            return int(value)
        return None

    def _map_question_value(self, path: str) -> Optional[str]:
        value = self._get_element_value(path)
        return {
            "0": "Überhaupt nicht",
            "1": "An einzelnen Tagen",
            "2": "An mehr als der Hälfte der Tage",
            "3": "Beinahe jeden Tag",
            "": "k.A."
        }.get(value.strip().lower()) if value else None

    def _map_difficulty_value(self, path: str) -> Optional[str]:
        value = self._get_element_value(path)
        return {
            "0": "Überhaupt nicht erschwert",
            "1": "Etwas erschwert",
            "2": "Relativ stark erschwert",
            "3": "Sehr stark erschwert",
            "": "k.A."
        }.get(value.strip().lower()) if value else None
