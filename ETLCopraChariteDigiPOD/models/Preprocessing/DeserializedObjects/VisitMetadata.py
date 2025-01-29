from dataclasses import dataclass, field

from datetime import datetime

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class VisitMetadata(BaseDeserializedObject):
    patnummer: int
    casenumber: int
    datetime: datetime
    opAnkernum: int
    provider: str
    place: str
    doknr: int
    #doktl: int
    type_: str
    deleted: bool
    file_id: str = field(init=False)

    def to_dict(self) -> dict:
        return {
            "file_id": [self.file_id],
            "patnummer": [self.patnummer],
            "casenumber": [self.casenumber],
            "visit_datetime": [self.datetime],
            "provider": [self.provider],
            "place": [self.place],
        }

    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()
