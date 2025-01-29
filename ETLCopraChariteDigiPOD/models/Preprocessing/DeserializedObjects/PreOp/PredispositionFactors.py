from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class PredispositionFactors(BaseDeserializedObject):
    advanced_age: Optional[bool]
    chronic_pain: Optional[bool]
    anxiety_disorder: Optional[bool]
    asa_grade_3: Optional[bool]
    dementia: Optional[bool]
    electrolyte_disorder: Optional[bool]
    dysphagia: Optional[bool]
    alcoholism: Optional[bool]
    anemia: Optional[bool]
    diabetes: Optional[bool]
    insulin_dependent: Optional[bool]
    parkinson: Optional[bool]
    sensory_deficit: Optional[bool]
    immunosuppression: Optional[bool]
    cardiac_disease: Optional[bool]
    vascular_disease: Optional[bool]
    stroke: Optional[bool]
    sleep_disorder: Optional[bool]

    def to_dict(self) -> dict:
        return {"praemed_rf_erhebung": self.isRiskFactorsAssessed, "praemed_rf_praedi": self.isRiskFactorsAssessed}

    @property
    def isRiskFactorsAssessed(self) -> int:
        for value in self.__dict__.values():
            if value:
                return 1
            else:
                return 0

    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()
