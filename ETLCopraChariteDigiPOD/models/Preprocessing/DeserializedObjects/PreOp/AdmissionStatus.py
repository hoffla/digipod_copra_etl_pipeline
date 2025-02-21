from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class AdmissionStatus(BaseDeserializedObject):
    gewicht: Optional[float]
    groesse: Optional[int]

    care_level: Optional[str]
    personal_hygiene: Optional[str]
    mobility: Optional[str]
    nutrition: Optional[str]
    nutritional_support: Optional[str]
    language_barrier: Optional[bool]
    communication_language: Optional[str]
    communication_language_other: Optional[str]
    interpreter_needed: Optional[bool]
    glasses: Optional[bool]
    dental_prosthesis: Optional[bool]
    hearing_aid: Optional[bool]
    speech_aid: Optional[bool]
    writing_tablet: Optional[bool]

    def to_dict(self) -> dict:
        return {"gewicht": [self.gewicht], "groesse": [self.groesse]}

    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()
