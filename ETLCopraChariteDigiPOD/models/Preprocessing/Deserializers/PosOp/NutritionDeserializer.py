from dataclasses import dataclass
from typing import Optional
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.DeserializedObjects.PosOp.Nutrition import Nutrition, Dysphagia, MouthHygiene
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper

basePath = './/SUB_DOC/SUB_DOC_CONTENT/'

@dataclass
class NutritionDeserializer(BaseDeserializer):
    def deserialize(self) -> Optional[Nutrition]:
        self_nutrition = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN069'))
        present = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN249'))
        oral = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN250'))
        gastric_tube = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN251'))
        peg = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN252'))

        surgical_contraind = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN254'))
        parenteral = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN323'))
        aspiration_risk = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN256'))
        pain = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN257'))
        reflux = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN258'))
        nausea = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN260'))

        return Nutrition(
            selfNutrition=self_nutrition,
            present=present,
            oral=oral,
            peg=peg,
            gastricTube=gastric_tube,
            surgicalContraInd=surgical_contraind,
            parenteral=parenteral,
            aspirationRisk=aspiration_risk,
            pain=pain,
            reflux=reflux,
            nausea=nausea
        )


@dataclass
class DysphagiaDeserializer(BaseDeserializer):
    def deserialize(self) -> Optional[Dysphagia]:
        present = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN261'))
        phonoaudio = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN262'))
        diet_change = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN263'))
        reason_diet_change = self._get_element_value(basePath + 'QVDELIN400')

        return Dysphagia(
            present=present,
            phonoaudio=phonoaudio,
            dietChange=diet_change,
            reasonDietChange=reason_diet_change
        )


@dataclass
class MouthHygieneDeserializer(BaseDeserializer):
    def deserialize(self) -> Optional[MouthHygiene]:
        present = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN265'))
        teeth_brushed = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN266'))
        mouth_wash = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN267'))
        gum_massage = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN268'))
        cheek_gymnastik = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN269'))

        return MouthHygiene(
            present=present,
            teethBrushed=teeth_brushed,
            mouthWash=mouth_wash,
            gumMassage=gum_massage,
            cheekGymnastik=cheek_gymnastik
        )
