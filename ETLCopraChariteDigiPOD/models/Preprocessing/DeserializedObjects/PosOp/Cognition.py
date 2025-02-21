from dataclasses import dataclass

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject
from models.Preprocessing.DeserializedObjects.PosOp.FearAnxiety import Drug


@dataclass
class CognitiveStimulation:
    present: bool
    reading: bool
    passiveUseDevices: bool
    activeUseDevices: bool
    games: bool
    conversation: bool
    singing: bool


@dataclass
class Company:
    present: bool
    family: bool
    trainedProfessional: bool
    volunteer: bool


@dataclass
class OrientationAid:
    present: bool
    clock: bool
    calender: bool
    tv: bool
    radio: bool
    newspaper: bool
    telefon: bool


@dataclass
class CommunicationAid:
    present: bool
    vision: bool
    audio: bool
    tablet: bool
    denture: bool
    speechAid: bool
    translator: bool


@dataclass
class CircadianNonPharmacological:
    present: bool
    sleepMask: bool
    earplugs: bool
    noiseReduction: bool
    emergentNightIntervention: bool
    dailyLightExposure: bool
    nightsLightReduction: bool
    closedDoor: bool
    sleepHygiene: bool


@dataclass
class CircadianPharmacological:
    present: bool
    drugs: [Drug]


@dataclass
class CircadianRhythmus:
    nonPharmacological: CircadianNonPharmacological
    pharmacological: CircadianPharmacological


@dataclass
class Cognition(BaseDeserializedObject):
    stimulation: CognitiveStimulation
    company: Company
    orientation: OrientationAid
    communication: CommunicationAid
    circadian: CircadianRhythmus

    def to_dict(self) -> dict:
        return {
            "kog_erfolgt": [self.stimulation.present], 
            "orientierung_erfolgt": [self.orientation.present],
            "kommuni_erfolgt": [self.communication.present],
            "circrhy_wie___1": [self.circadian.nonPharmacological.present],
            }

    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()
