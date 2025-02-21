from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class TimedUpAndGoScore(BaseDeserializedObject):
    value: Optional[float]
    not_collected: Optional[bool]

    def to_dict(self) -> dict:
        return {"tug_summe": [self.value]}

    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()
