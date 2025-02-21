from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class AlkoholkonsumScore(BaseDeserializedObject):
    total_score: Optional[int]
    question_1: Optional[str]
    question_2: Optional[str]
    question_3: Optional[str]
    not_collected: Optional[bool]
    years_not_drinking: Optional[int]

    def to_dict(self) -> dict:
        return {"auditc": [self.total_score]}

    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()
