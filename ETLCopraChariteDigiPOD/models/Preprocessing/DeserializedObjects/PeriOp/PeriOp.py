from dataclasses import dataclass
from typing import Optional
from datetime import datetime

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class PerioOP(BaseDeserializedObject):
    eeg: Optional[bool]
    regional_anaesthesia: Optional[bool]
    fasting_drink_time: Optional[datetime]
    fasting_food_time: Optional[datetime]
    high_caloric_fluid: Optional[str]
    medication_administered: Optional[str]
    medication_reason: Optional[str]
    medication: Optional[str]
    medication_unit: Optional[str]
    medication_dose: Optional[float]
    medication_time: Optional[datetime]

    def to_dict(self) -> dict:
        return dict()

    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()
