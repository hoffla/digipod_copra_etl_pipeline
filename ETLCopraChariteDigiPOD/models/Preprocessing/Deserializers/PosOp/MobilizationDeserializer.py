from dataclasses import dataclass
from typing import Optional
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.DeserializedObjects.PosOp.Mobilization import Mobilization
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper

basePath = './/SUB_DOC/SUB_DOC_CONTENT/'

@dataclass
class MobilizationDeserializer(BaseDeserializer):
    def deserialize(self) -> Optional[Mobilization]:
        self_mobile = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN066'))
        present = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN233'))
        intensity = self._get_element_value(basePath + 'QVDELIN294')
        support = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN240'))
        aid = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN324'))

        surgical_contraind = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN242'))
        pain = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN245'))
        strengthless = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN246'))
        exaustion = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value(basePath + 'QVDELIN247'))
        miscellaneous = self._get_element_value(basePath + 'QVDELIN321') or self._get_element_value(basePath + 'QVDELIN248')

        return Mobilization(
            selfMobile=self_mobile,
            present=present,
            intensity=intensity,
            support=support,
            aid=aid,
            surgicalContraInd=surgical_contraind,
            pain=pain,
            strengthless=strengthless,
            exaustion=exaustion,
            miscellaneous=miscellaneous
        )
