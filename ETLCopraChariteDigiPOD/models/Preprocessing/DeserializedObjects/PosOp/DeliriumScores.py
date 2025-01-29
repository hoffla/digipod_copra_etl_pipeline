from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class DeliriumScore(BaseDeserializedObject):
    type_: str
    datetime: datetime
    score: Optional[int]
    score_text: str

    def to_dict(self) -> dict:
        return {
            "delirtest_typ": [self.type_],
            "delirtest_datetime": [self.datetime],
            "delirtest_result2": [self.score_text],
            "delirtest_result": [self.score]
        }


@dataclass
class NuDescScore(DeliriumScore):
    desorientation: int
    inappropriate_behavior: int
    inappropriate_communication: int
    hallucinations: int
    psychomotor_retardation: int

    @property
    def name(self) -> str:
        parent_class = self.__class__.__base__
        return parent_class.__name__.lower()


@dataclass
class CamIcuScore(DeliriumScore):
    notPossible: Optional[bool]
    reasonNotPossible: Optional[str]
    feature1a: Optional[bool]
    feature1b: Optional[bool]
    feature2a: Optional[int]
    feature2b: Optional[int]
    feature3: Optional[int]
    feature4a: Optional[int]
    feature4b: Optional[int]

    @property
    def name(self) -> str:
        parent_class = self.__class__.__base__
        return parent_class.__name__.lower()


@dataclass
class RassScore(DeliriumScore):
    pass

    @property
    def name(self) -> str:
        parent_class = self.__class__.__base__
        return parent_class.__name__.lower()


@dataclass
class GCSScore(DeliriumScore):
    eye_reaction: str
    voice_reaction: str
    motor_reaction: str

    @property
    def name(self) -> str:
        parent_class = self.__class__.__base__
        return parent_class.__name__.lower()


@dataclass
class ICDSCScore(DeliriumScore):
    modi_conscientiousness: int
    inattention: int
    desorientation: int
    hallucinations: int
    psychomotor: int
    inappropriate_communication: int
    circ_rhythmus_disfunc: int
    oscillatory_symptomatic: int

    @property
    def name(self) -> str:
        parent_class = self.__class__.__base__
        return parent_class.__name__.lower()


@dataclass
class DDS8Score(DeliriumScore):
    orientation: int
    hallucination: int
    agitation: int
    fear: int
    myoclonia: int
    paroxysmal_sweating: int
    sleep_disorder: int
    tremor: int

    @property
    def name(self) -> str:
        parent_class = self.__class__.__base__
        return parent_class.__name__.lower()


@dataclass
class DOSScore(DeliriumScore):
    sleep: Optional[int]
    distraction: Optional[int]
    attention: Optional[int]
    question: Optional[int]
    response: Optional[int]
    retardation: Optional[int]
    orientation: Optional[int]
    daytime: Optional[int]
    memory: Optional[int]
    restlessness: Optional[int]
    removalForeign: Optional[int]
    emotion: Optional[int]
    hallucination: Optional[int]

    @property
    def name(self) -> str:
        parent_class = self.__class__.__base__
        return parent_class.__name__.lower()
