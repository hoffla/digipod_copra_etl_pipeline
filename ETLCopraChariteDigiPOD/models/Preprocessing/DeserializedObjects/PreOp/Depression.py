from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class DepressionScore(BaseDeserializedObject):
    score: int
    not_collected: Optional[bool]
    question_a: Optional[str]
    question_b: Optional[str]
    question_c: Optional[str]
    question_d: Optional[str]
    question_e: Optional[str]
    question_f: Optional[str]
    question_g: Optional[str]
    question_h: Optional[str]
    difficulty: Optional[str]

    def to_dict(self) -> dict:
        return {"phq_summe": self.score}

    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()
