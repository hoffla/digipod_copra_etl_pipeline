from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class PolypharmacyScore(BaseDeserializedObject):
    score: int
    not_collected: Optional[bool]

    def to_dict(self) -> dict:
        return {"polypharmacy_summe": self.score}

    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()
