from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class FrailtyScore(BaseDeserializedObject):
    total_pathological_criteria: int
    criterion_1: Optional[bool]
    criterion_1_weight: Optional[float]
    criterion_2: Optional[bool]
    criterion_2_frequency: Optional[str]
    criterion_3: Optional[bool]
    criterion_3_metabolism: Optional[str]
    criterion_4: Optional[bool]
    criterion_4_hand_strength_1: Optional[float]
    criterion_4_hand_strength_2: Optional[float]
    criterion_4_hand_strength_3: Optional[float]
    criterion_4_hand_strength_not_possible: Optional[bool]
    criterion_5: Optional[bool]
    criterion_5_seconds: Optional[float]
    criterion_5_not_possible: Optional[bool]
    not_collected: Optional[bool]

    def to_dict(self) -> dict:
        return {"frailty": self.total_pathological_criteria, "frailty_criteria": self.getFrailtyCriteria}

    @property
    def getFrailtyCriteria(self) -> str:
        if self.total_pathological_criteria >= 2:
            return 'Frail'
        elif self.total_pathological_criteria > 0:
            return 'Pre-Frail'
        else:
            return 'Non Frail'

    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()
