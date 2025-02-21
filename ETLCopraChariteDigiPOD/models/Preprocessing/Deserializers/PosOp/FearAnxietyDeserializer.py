from dataclasses import dataclass
from typing import Optional, List

from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.DeserializedObjects.PosOp.FearAnxiety import Anxiety, FAS, AnxietyNonPharmacological, \
    AnxietyPharmacological, Drug
from models.Preprocessing.Utils.DateTimeParser import DateTimeParser
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper

basePath = './/SUB_DOC/SUB_DOC_CONTENT/'


drugMapping = {
    'Clonidin': {'used': 'QVDELIN278', 'dosis': 'QVDELIN282', 'einheit': 'QVDELIN470', 'datum': 'QVDELIN301', 'zeit': 'QVDELIN302'},
    'Lorazepam': {'used': 'QVDELIN279', 'dosis': 'QVDELIN283', 'einheit': 'QVDELIN471', 'datum': 'QVDELIN303', 'zeit': 'QVDELIN304'},
    'Lormetazepam': {'used': 'QVDELIN280', 'dosis': 'QVDELIN284', 'einheit': 'QVDELIN472', 'datum': 'QVDELIN305', 'zeit': 'QVDELIN306'},
    'Diazepam': {'used': 'QVDELIN281', 'dosis': 'QVDELIN285', 'einheit': 'QVDELIN473', 'datum': 'QVDELIN307', 'zeit': 'QVDELIN308'},
}


@dataclass
class AnxietyDeserializer(BaseDeserializer):
    def deserialize(self) -> List[Anxiety]:
        fas = self._deserialize_fas()

        stress_reduction = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN159'))

        nonpharma_stress_reduction = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN160'))
        verbal_stress_reduction = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN271'))
        integration_family = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN273'))
        social_service = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN274'))
        paliative_service = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN275'))
        explanation_procedures = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN276'))
        pat_wishes = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN277'))

        non_pharmacological = AnxietyNonPharmacological(
            present=nonpharma_stress_reduction,
            verbalStressReduction=verbal_stress_reduction,
            integrationFamily=integration_family,
            socialService=social_service,
            paliativeService=paliative_service,
            explanationProcedures=explanation_procedures,
            patWishes=pat_wishes
        )

        clonidin = self._deserialize_drug('Clonidin')
        lorazepam = self._deserialize_drug('Lorazepam')
        lormetazepam = self._deserialize_drug('Lormetazepam')
        diazepam = self._deserialize_drug('Diazepam')

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
    
    def _deserialize_fas(self):
        '''
        Stand 21/02/2025: somente o último faz é mandado para o XML Doc, dessa forma tento apenas pegar esse elemento!
        Como o elemento 157 sempre está presente (mesmo que vazio) utilizo o try except por que do erro dentro do parse_datetime
        '''
        items = self.navigator.find_elements('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN157')

        for item in items:
            try:
                fas_score_string = self.navigator.get_element_value(item.find('QVDELIN158'), 'VALUE', element_nullable=True)
                fas_datetime_string = self.navigator.get_element_value(item.find('X00ELIN155'), 'VALUE', element_nullable=True)
                fas_time_string = self.navigator.get_element_value(item.find('X00ELIN156'), 'VALUE', element_nullable=True)

                fas_datetime = DateTimeParser.parse_datetime(fas_datetime_string, fas_time_string, 'FAS')
                fas_score = int(fas_score_string) if fas_score_string else None
                fas = FAS(score=fas_score, datetime=fas_datetime)
                return fas

            except ValueError:
                pass

    def _deserialize_drug(self, drug_name) -> Optional[Drug]:
        drugItems = drugMapping.get(drug_name)
        drug_used = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + drugItems.get('used')))
        

        if drug_used:
            drug_dosis_string = self._get_element_value(basePath + drugItems.get('dosis'))
            drug_unit_string = self._get_element_value(basePath + drugItems.get('dosis'))
            drug_date_string = self._get_element_value(basePath + drugItems.get('datum'))
            drug_time_string = self._get_element_value(basePath + drugItems.get('zeit'))
            drug_datetime = DateTimeParser.parse_datetime(drug_date_string, drug_time_string, drug_name)

            return Drug(
                name=drug_name,
                dose=float(drug_dosis_string) if drug_dosis_string else 0.0,
                unit=drug_unit_string,
                route="oral",
                datetime=drug_datetime
            )