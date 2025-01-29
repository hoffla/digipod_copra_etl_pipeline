from dataclasses import dataclass
from typing import List, Optional

from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.DeserializedObjects.PosOp.Cognition import Cognition, CognitiveStimulation, Company, OrientationAid, \
    CommunicationAid, CircadianRhythmus, CircadianPharmacological, CircadianNonPharmacological
from models.Preprocessing.DeserializedObjects.PosOp.FearAnxiety import Drug
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper
from models.Preprocessing.Utils.DateTimeParser import DateTimeParser

basePath = './/SUB_DOC/SUB_DOC_CONTENT/'


@dataclass
class CognitionDeserializer(BaseDeserializer):
    def deserialize(self) -> Optional[Cognition]:
        stimulation = self._deserialize_stimulation()
        company = self._deserialize_company()
        orientation = self._deserialize_orientation()
        communication = self._deserialize_communication()
        circadian = self._deserialize_circadian()

        return Cognition(
            stimulation=stimulation,
            company=company,
            orientation=orientation,
            communication=communication,
            circadian=circadian
        )

    def _deserialize_stimulation(self) -> CognitiveStimulation:
        present = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN180'))
        reading = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN182'))
        passive_devices = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN183'))
        active_devices = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN184'))
        games = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN185'))
        conversation = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN186'))
        singing = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN187'))

        return CognitiveStimulation(
            present=present,
            reading=reading,
            passiveUseDevices=passive_devices,
            activeUseDevices=active_devices,
            games=games,
            conversation=conversation,
            singing=singing
        )

    def _deserialize_company(self) -> Company:
        present = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN188'))
        family = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN190'))
        trained_professional = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN191'))
        volunteer = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN192'))

        return Company(
            present=present,
            family=family,
            trainedProfessional=trained_professional,
            volunteer=volunteer
        )

    def _deserialize_orientation(self) -> OrientationAid:
        present = XMLDeserializerHelper.determine_yes_no_ka_value(self._get_element_value(basePath + 'QVDELIN193'))
        clock = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN194'))
        calender = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN195'))
        tv = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN196'))
        radio = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN197'))
        newspaper = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN198'))
        telefon = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN199'))

        return OrientationAid(
            present=present,
            clock=clock,
            calender=calender,
            tv=tv,
            radio=radio,
            newspaper=newspaper,
            telefon=telefon
        )

    def _deserialize_communication(self) -> CommunicationAid:
        present = XMLDeserializerHelper.determine_yes_no_ka_value(self._get_element_value(basePath + 'QVDELIN200'))
        vision = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN201'))
        audio = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN202'))
        tablet = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN203'))
        denture = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN204'))
        speech_aid = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN205'))
        translator = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN206'))

        return CommunicationAid(
            present=present,
            vision=vision,
            audio=audio,
            tablet=tablet,
            denture=denture,
            speechAid=speech_aid,
            translator=translator
        )

    def _deserialize_circadian(self) -> CircadianRhythmus:
        non_pharmacological = self._deserialize_circadian_non_pharmacological()
        pharmacological = self._deserialize_circadian_pharmacological()

        return CircadianRhythmus(
            nonPharmacological=non_pharmacological,
            pharmacological=pharmacological
        )

    def _deserialize_circadian_non_pharmacological(self) -> CircadianNonPharmacological:
        present = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN207'))
        sleep_mask = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN209'))
        earplugs = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN210'))
        noise_reduction = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN211'))
        emergent_night_intervention = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN212'))
        daily_light_exposure = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN213'))
        nights_light_reduction = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN214'))
        closed_door = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN215'))
        sleep_hygiene = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN216'))

        return CircadianNonPharmacological(
            present=present,
            sleepMask=sleep_mask,
            earplugs=earplugs,
            noiseReduction=noise_reduction,
            emergentNightIntervention=emergent_night_intervention,
            dailyLightExposure=daily_light_exposure,
            nightsLightReduction=nights_light_reduction,
            closedDoor=closed_door,
            sleepHygiene=sleep_hygiene
        )

    def _deserialize_circadian_pharmacological(self) -> CircadianPharmacological:
        present = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN217'))
        drugs = self._deserialize_drugs()

        return CircadianPharmacological(
            present=present,
            drugs=drugs
        )

    def _deserialize_drugs(self) -> List[Drug]:
        drugs = []

        for i in range(1, 6):  # Supondo que existem até 5 medicamentos
            drug_name = self._get_element_value(basePath + f'QVDELIN22{i}', 'value', nullable=True)
            if drug_name:
                drug_dose = float(self._get_element_value(basePath + f'QVDELIN22{i + 1}', 'value', nullable=True) or 0)
                drug_unit = self._get_element_value(basePath + f'QVDELIN47{i}', 'value', nullable=True)
                drug_date = DateTimeParser.parse_datetime(
                    self._get_element_value(basePath + f'QVDELIN31{i + 9}', 'value', nullable=True),
                    self._get_element_value(basePath + f'QVDELIN31{i + 10}', 'value', nullable=True),
                    f'Drug {i}'
                )
                drugs.append(Drug(name=drug_name, dose=drug_dose, unit=drug_unit, route='oral', datetime=drug_date))

        return drugs
