from dataclasses import dataclass
from typing import Optional, List

from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.DeserializedObjects.PosOp.FearAnxiety import Anxiety, FAS, AnxietyNonPharmacological, \
    AnxietyPharmacological, Drug
from models.Preprocessing.Utils.DateTimeParser import DateTimeParser
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper


@dataclass
class AnxietyDeserializer(BaseDeserializer):
    def deserialize(self) -> List[Anxiety]:
        anxiety_items = []

        items = self.navigator.find_elements('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN157')

        for item in items:
            anxiety = self._deserialize_anxiety(item)
            anxiety_items.append(anxiety)

        return anxiety_items

    def _deserialize_anxiety(self, item_element):
        fas_score_string = self.navigator.get_element_value(item_element.find('QVDELIN158'), 'VALUE', element_nullable=True)
        fas_datetime_string = self.navigator.get_element_value(item_element.find('X00ELIN155'), 'VALUE', element_nullable=True)
        fas_time_string = self.navigator.get_element_value(item_element.find('X00ELIN156'), 'VALUE', element_nullable=True)

        fas_datetime = DateTimeParser.parse_datetime(fas_datetime_string, fas_time_string, 'FAS')
        fas_score = int(fas_score_string) if fas_score_string else None

        fas = FAS(score=fas_score, datetime=fas_datetime)

        stress_reduction = XMLDeserializerHelper.determine_yes_no_value(self.navigator.get_element_value(item_element.find('QVDELIN159'), element_nullable=True))

        nonpharma_stress_reduction = XMLDeserializerHelper.determine_yes_no_value(self.navigator.get_element_value(item_element.find('QVDELIN160'), element_nullable=True))
        verbal_stress_reduction = XMLDeserializerHelper.determine_yes_no_value(self.navigator.get_element_value(item_element.find('QVDELIN271'), element_nullable=True))
        integration_family = XMLDeserializerHelper.determine_yes_no_value(self.navigator.get_element_value(item_element.find('QVDELIN273'), element_nullable=True))
        social_service = XMLDeserializerHelper.determine_yes_no_value(self.navigator.get_element_value(item_element.find('QVDELIN274'), element_nullable=True))
        paliative_service = XMLDeserializerHelper.determine_yes_no_value(self.navigator.get_element_value(item_element.find('QVDELIN275'), element_nullable=True))
        explanation_procedures = XMLDeserializerHelper.determine_yes_no_value(self.navigator.get_element_value(item_element.find('QVDELIN276'), element_nullable=True))
        pat_wishes = XMLDeserializerHelper.determine_yes_no_value(self.navigator.get_element_value(item_element.find('QVDELIN277'), element_nullable=True))

        non_pharmacological = AnxietyNonPharmacological(
            present=nonpharma_stress_reduction,
            verbalStressReduction=verbal_stress_reduction,
            integrationFamily=integration_family,
            socialService=social_service,
            paliativeService=paliative_service,
            explanationProcedures=explanation_procedures,
            patWishes=pat_wishes
        )

        clonidin = self._deserialize_drug(item_element, 'QVDELIN278', 'Clonidin')
        lorazepam = self._deserialize_drug(item_element, 'QVDELIN279', 'Lorazepam')
        lormetazepam = self._deserialize_drug(item_element, 'QVDELIN280', 'Lormetazepam')
        diazepam = self._deserialize_drug(item_element, 'QVDELIN281', 'Diazepam')

        pharmacological = AnxietyPharmacological(
            clonidin=clonidin,
            lorazepam=lorazepam,
            lormetazepam=lormetazepam,
            diazepam=diazepam
        )

        return Anxiety(
            fas=fas,
            stressReduction=stress_reduction,
            nonPharmacological=non_pharmacological,
            pharmacological=pharmacological,
        )

    def _deserialize_drug(self, item_element, drug_id, drug_name) -> Optional[Drug]:
        drug_used = XMLDeserializerHelper.determine_yes_no_value(self.navigator.get_element_value(item_element.find(drug_id), element_nullable=True))

        if drug_used:
            drug_dosis_string = self.navigator.get_element_value(item_element.find(f'{drug_id}Dosis'), 'VALUE')
            drug_unit_string = self.navigator.get_element_value(item_element.find(f'{drug_id}Einheit'), 'VALUE')
            drug_date_string = self.navigator.get_element_value(item_element.find(f'{drug_id}Datum'), 'VALUE')
            drug_time_string = self.navigator.get_element_value(item_element.find(f'{drug_id}Zeit'), 'VALUE')
            drug_datetime = DateTimeParser.parse_datetime(drug_date_string, drug_time_string, drug_name)

            return Drug(
                name=drug_name,
                dose=float(drug_dosis_string) if drug_dosis_string else 0.0,
                unit=drug_unit_string,
                route="oral",
                datetime=drug_datetime
            )
