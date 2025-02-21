from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class FallRisk(BaseDeserializedObject):
    gestuerzt: Optional[bool]
    anzahl: Optional[int]
    nicht_erhoben: Optional[bool]

    def to_dict(self) -> dict:
        return {"fallrisk": [self.gestuerzt]}

    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()
