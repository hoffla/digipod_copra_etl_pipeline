from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.PreOp.SocialRisk import SocialSituationScore
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper


@dataclass
class SozialeSituationScoreDeserializer(BaseDeserializer):
    def deserialize(self) -> SocialSituationScore:
        total_score = self._parse_int_value('QVDELIN373')
        not_collected = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN418'))
        question_0 = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN419'))

        question_1 = self._map_value(self._get_element_value('QVDELIN420'), {
            "11": "Schon lange allein", "20": "Seit < 1 Jahr allein",
            "31": "bei Familienangehörigen oder mit rüstigem Partner",
            "40": "mit Partner, der selbst Hilfe braucht"
        })

        question_2 = self._map_value(self._get_element_value('QVDELIN421'), {
            "0": "Nein", "1": "Ja", "": "k.A."
        })

        question_3 = self._map_value(self._get_element_value('QVDELIN422'), {
            "11": "mehrmals täglich/jeden Tag", "21": "ein-/mehrmalig in der Woche",
            "30": "selten (ein bis zweimal im Monat)", "40": "(fast) nie", "": "k.A."
        })

        question_4 = self._map_value(self._get_element_value('QVDELIN423'), {
            "0": "Beziehung teilweise konfliktbeladen und gespannt", "1": "Beziehung harmonisch und vertrauensvoll",
            "": "k.A."
        })

        question_5 = self._map_value(self._get_element_value('QVDELIN424'), {
            "11": "Habe neue Bekannte gewonnen", "21": "keine Veränderung",
            "30": "Einige Kontakte habe ich aufgeben müssen", "40": "Habe nahezu alle wichtigen Kontakte verloren",
            "": "k.A."
        })

        question_6 = self._map_value(self._get_element_value('QVDELIN425'), {
            "11": "Fühle mich rundum gut versorgt", "21": "Es geht so, man muss zufrieden sein",
            "30": "Fühle mich einsam und im Stich gelassen", "": "k.A."
        })

        return SocialSituationScore(
            total_score=total_score,
            not_collected=not_collected,
            question_0=question_0,
            question_1=question_1,
            question_2=question_2,
            question_3=question_3,
            question_4=question_4,
            question_5=question_5,
            question_6=question_6
        )

    def _parse_int_value(self, path: str) -> Optional[int]:
        value = self._get_element_value(path)
        try:
            return int(value) if value else None
        except ValueError:
            return None

    def _map_value(self, value: Optional[str], mapping: dict) -> Optional[str]:
        return mapping.get(value.strip().lower()) if value else None
