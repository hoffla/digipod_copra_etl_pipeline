from dataclasses import dataclass
from typing import Optional
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.DeserializedObjects.PosOp.Nutrition import Nutrition, Dysphagia, MouthHygiene
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper


@dataclass
class NutritionDeserializer(BaseDeserializer):
    def deserialize(self) -> Optional[Nutrition]:
        self_nutrition = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN069'))
        present = XMLDeserializerHelper.determine_yes_no_ka_value(self._get_element_value('QVDELIN249'))
        oral = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN250'))
        gastric_tube = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN251'))
        peg = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN252'))

        surgical_contraind = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN254'))
        parenteral = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN323'))
        aspiration_risk = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN256'))
        pain = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN257'))
        reflux = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN258'))
        nausea = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN260'))

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
        present = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN255'))
        phonoaudio = XMLDeserializerHelper.determine_yes_no_ka_value(self._get_element_value('QVDELIN262'))
        diet_change = XMLDeserializerHelper.determine_yes_no_ka_value(self._get_element_value('QVDELIN263'))
        reason_diet_change = self._get_element_value('QVDELIN400')

        return Dysphagia(
            present=present,
            phonoaudio=phonoaudio,
            dietChange=diet_change,
            reasonDietChange=reason_diet_change
        )


@dataclass
class MouthHygieneDeserializer(BaseDeserializer):
    def deserialize(self) -> Optional[MouthHygiene]:
        present = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN265'))
        teeth_brushed = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN266'))
        mouth_wash = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN267'))
        gum_massage = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN268'))
        cheek_gymnastik = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN269'))

        return MouthHygiene(
            present=present,
            teethBrushed=teeth_brushed,
            mouthWash=mouth_wash,
            gumMassage=gum_massage,
            cheekGymnastik=cheek_gymnastik
        )
