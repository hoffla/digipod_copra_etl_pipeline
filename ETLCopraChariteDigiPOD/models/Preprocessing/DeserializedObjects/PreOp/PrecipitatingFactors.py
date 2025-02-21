from dataclasses import dataclass

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class PrecipitatingFactors(BaseDeserializedObject):
    expected_op_duration: bool
    abdominal_procedure: bool
    intrathoracic_procedure: bool
    major_surgery: bool
    expected_severe_postoperative_pain: bool
    contraindication: bool
    expected_transfusion_of_blood_products: bool

    def to_dict(self) -> dict:
        return {"praemed_rf_praezi": [self.isRiskFactorsAssessed]}

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
