from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class MiniCog(BaseDeserializedObject):
    result: int
    word_score: Optional[int]
    clock_score: Optional[str]
    not_recorded: Optional[bool]
    reason_not_recorded: Optional[str]
    word_version: Optional[str]

    def to_dict(self) -> dict:
        return {"cog_minicog_sum": [self.result]}

    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()

