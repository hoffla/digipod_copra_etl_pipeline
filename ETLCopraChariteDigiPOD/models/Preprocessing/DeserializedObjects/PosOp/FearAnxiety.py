from dataclasses import dataclass
from datetime import datetime

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class AnxietyNonPharmacological:
    present: bool
    verbalStressReduction: bool
    integrationFamily: bool
    socialService: bool
    paliativeService: bool
    explanationProcedures: bool
    patWishes: bool


@dataclass
class Drug:
    name: str
    dose: float
    unit: str
    route: str
    datetime: datetime


@dataclass
class AnxietyPharmacological:
    diazepam: Drug
    lormetazepam: Drug
    lorazepam: Drug
    clonidin: Drug

    @property
    def present(self):
        for att_value in self.__dict__.values():
            if att_value:
                return True
        return False


@dataclass
class FAS:
    score: int
    datetime: datetime


@dataclass
class Anxiety(BaseDeserializedObject):
    fas: FAS
    stressReduction: bool
    nonPharmacological: AnxietyNonPharmacological
    pharmacological: AnxietyPharmacological

    def to_dict(self) -> dict:
        return {
            "fas_score": [self.fas.score],
            "fas_datetime": [self.fas.datetime],
            "angst_bewaltigung_typ___1": [self.nonPharmacological.present],
            "angst_bewaltigung_typ___2": [self.pharmacological.present],
        }
