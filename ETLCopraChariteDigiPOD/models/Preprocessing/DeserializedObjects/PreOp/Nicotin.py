from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class NicotinConsumptionScore(BaseDeserializedObject):
    total_score: int
    question1: Optional[str]
    question1_years: Optional[int]
    question2: Optional[str]
    question2_amount: Optional[int]
    question3: Optional[int]
    question4: Optional[str]
    not_collected: Optional[bool]

    def to_dict(self) -> dict:
        return {"nikotin_summe": [self.total_score]}

    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()

