from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class PainScore(BaseDeserializedObject):
    type_: str
    datetime: datetime
    score: int

    def to_dict(self) -> dict:
        return {f"{self.type_}_datetime": [self.datetime], f"{self.type_}_result": [self.score]} #Todo: vou ter problema aqui, pois posso ter dois scores iguais na mesma visita!


@dataclass
class NRS(PainScore):
    tolerable: Optional[bool]
    condition: Optional[str]
    local: Optional[str]
    quality: Optional[str]

    @property
    def name(self) -> str:
        parent_class = self.__class__.__base__
        return parent_class.__name__.lower()


@dataclass
class BPS(PainScore):
    face: int
    upperExt: int
    adaptationVent: int

    @property
    def name(self) -> str:
        parent_class = self.__class__.__base__
        return parent_class.__name__.lower()


@dataclass
class BPSNI(PainScore):
    face: int
    upperExt: int
    vocalisation: int

    @property
    def name(self) -> str:
        parent_class = self.__class__.__base__
        return parent_class.__name__.lower()


@dataclass
class BESD(PainScore):
    condition: Optional[str]
    breathing: int
    negativeVocalisation: int
    face: int
    bodyLanguage: int
    consolation: int

    @property
    def name(self) -> str:
        parent_class = self.__class__.__base__
        return parent_class.__name__.lower()
