from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class ForeignObject(BaseDeserializedObject):
    type_: str
    datetime: datetime
    removal_datetime: Optional[datetime]
    indicationproof_datetime: Optional[datetime]
    indicationproof: Optional[bool]

    def to_dict(self) -> dict:
        return {"fremdmaterial": [self.type_], "indikationspruefung": [self.indicationproof]}
