from dataclasses import dataclass
from typing import Optional


from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class Nutrition(BaseDeserializedObject):
    selfNutrition: bool
    present: bool
    oral: bool
    peg: bool
    gastricTube: bool

    surgicalContraInd: bool
    parenteral: bool
    aspirationRisk: bool
    pain: bool
    reflux: bool
    nausea: bool

    def to_dict(self) -> dict:
        return {"nutri_erfolgt": [self.present]}
    
    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()


@dataclass
class Dysphagia(BaseDeserializedObject):
    present: bool
    phonoaudio: bool
    dietChange: bool
    reasonDietChange: Optional[str]

    def to_dict(self) -> dict:
        return {"schluck_behandlung": [self.phonoaudio], "schluck_nutri_umstellung": [self.dietChange]}
    
    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()


@dataclass
class MouthHygiene(BaseDeserializedObject):
    present: bool
    teethBrushed: bool
    mouthWash: bool
    gumMassage: bool
    cheekGymnastik: bool

    def to_dict(self) -> dict:
        return {"mundhyg_erfolgt": [self.present]}
    
    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()
