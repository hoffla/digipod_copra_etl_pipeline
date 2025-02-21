from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class Mobilization(BaseDeserializedObject):
    selfMobile: bool
    present: bool
    intensity: str
    support: bool
    aid: bool

    surgicalContraInd: bool
    pain: bool
    strengthless: bool
    exaustion: bool
    miscellaneous: Optional[str]

    def to_dict(self) -> dict:
        return {"mobil_erfolgt": [self.present]}
    
    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()
