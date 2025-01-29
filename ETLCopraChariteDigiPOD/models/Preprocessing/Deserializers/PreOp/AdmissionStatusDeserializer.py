from dataclasses import dataclass
from typing import Optional, Dict
from models.Preprocessing.DeserializedObjects.PreOp.AdmissionStatus import AdmissionStatus
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper


@dataclass
class AdmissionStatusDeserializer(BaseDeserializer):
    def deserialize(self) -> AdmissionStatus:
        mapping_dict = self._get_mappings()

        gewicht = self._parse_decimal_value('QVDELIN450')
        groesse = self._parse_long_value('QVDELIN451')

        care_level = self._map_value('QVDELIN052', mapping_dict['care_level'])
        personal_hygiene = self._map_value('QVDELIN053', mapping_dict['personal_hygiene'])
        mobility = self._map_value('QVDELIN054', mapping_dict['mobility'])
        nutrition = self._map_value('QVDELIN055', mapping_dict['nutrition'])
        nutritional_support = self._map_value('QVDELIN349', mapping_dict['nutritional_support'])

        language_barrier = self._deserialize_yes_no('QVDELIN056')
        communication_language = self._map_value('QVDELIN337', mapping_dict['communication_language'])
        communication_language_other = self._get_element_value('QVDELIN057')
        interpreter_needed = self._deserialize_yes_no('X00ELIN043')

        glasses = self._deserialize_yes_no('QVDELINHilfsmittelBrille')
        dental_prosthesis = self._deserialize_yes_no('QVDELINHilfsmittelZahnprothese')
        hearing_aid = self._deserialize_yes_no('QVDELINHilfsmittelHörgerät')
        speech_aid = self._deserialize_yes_no('QVDELINHilfsmittelSprachhilfe')
        writing_tablet = self._deserialize_yes_no('QVDELINHilfsmittelSchreibtafel')

        return AdmissionStatus(
            gewicht=gewicht,
            groesse=groesse,
            care_level=care_level,
            personal_hygiene=personal_hygiene,
            mobility=mobility,
            nutrition=nutrition,
            nutritional_support=nutritional_support,
            language_barrier=language_barrier,
            communication_language=communication_language,
            communication_language_other=communication_language_other,
            interpreter_needed=interpreter_needed,
            glasses=glasses,
            dental_prosthesis=dental_prosthesis,
            hearing_aid=hearing_aid,
            speech_aid=speech_aid,
            writing_tablet=writing_tablet
        )

    @staticmethod
    def _get_mappings() -> Dict[str, Dict[str, str]]:
        return {
            'care_level': {"1": "1", "2": "2", "3": "3", "4": "4", "5": "5", "NULL": "k.A."},
            'personal_hygiene': {"0": "Selbständig", "1": "benötigt Hilfe", "2": "Übernahme durch Pflege", "NULL": "k.A."},
            'mobility': {"0": "Selbständig", "1": "mit Hilfsmitteln", "2": "bettlägerig", "NULL": "k.A."},
            'nutrition': {"0": "Selbständig", "1": "mit Hilfe", "NULL": "k.A."},
            'nutritional_support': {"1": "oral", "2": "über Magensonde", "3": "über PEG", "4": "parenteral", "NULL": "k.A."},
            'communication_language': {
                "de": "deutsch", "tr": "türkisch", "ar": "arabisch", "it": "italienisch",
                "es": "spanisch", "fr": "französisch", "ru": "russisch", "pl": "polnisch"
            }
        }

    def _map_value(self, path: str, mapping: dict) -> Optional[str]:
        value = self._get_element_value(path)
        return mapping.get(value.strip().upper()) if value else None

    def _deserialize_yes_no(self, path: str) -> Optional[bool]:
        return XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(path))

    def _parse_decimal_value(self, path: str) -> Optional[float]:
        value = self._get_element_value(path)
        try:
            return float(value) if value else None
        except ValueError:
            return None

    def _parse_long_value(self, path: str) -> Optional[int]:
        value = self._get_element_value(path)
        try:
            return int(value) if value else None
        except ValueError:
            return None
