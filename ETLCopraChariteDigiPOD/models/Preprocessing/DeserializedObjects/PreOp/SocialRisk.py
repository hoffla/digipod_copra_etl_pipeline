from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class SocialSituationScore(BaseDeserializedObject):
    total_score: int
    not_collected: Optional[bool]
    question_0: Optional[bool]
    question_1: Optional[str]
    question_2: Optional[str]
    question_3: Optional[str]
    question_4: Optional[str]
    question_5: Optional[str]
    question_6: Optional[str]

    def to_dict(self) -> dict:
        return {"sos_summe": [self.total_score]}

    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()
