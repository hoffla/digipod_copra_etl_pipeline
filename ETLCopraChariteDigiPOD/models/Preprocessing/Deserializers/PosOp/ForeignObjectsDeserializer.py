from dataclasses import dataclass
from typing import List

from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.DeserializedObjects.PosOp.ForeignObject import ForeignObject
from models.Preprocessing.Utils.DateTimeParser import DateTimeParser
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper


@dataclass
class ForeignObjectsDeserializer(BaseDeserializer):
    def deserialize(self) -> List[ForeignObject]:
        fremdmaterial_items = []

        items = self.navigator.find_elements('.//MAIN_DOC_CONTENT/QVDELIN172/ITEM')

        for item in items:
            fremdmaterial = self._deserialize_fremdmaterial(item)
            fremdmaterial_items.append(fremdmaterial) if fremdmaterial else None

        return fremdmaterial_items

    def _deserialize_fremdmaterial(self, item_element):
        try:
            type_string = self.navigator.get_element_value(item_element.find('QVDELIN173'), 'VALUE', element_nullable=True)
            indate_string = self.navigator.get_element_value(item_element.find('QVDELIN174'), 'VALUE', element_nullable=True) # Todo: talvez seja possível nao ter isso documentado!!!
            intime_string = self.navigator.get_element_value(item_element.find('QVDELIN175'), 'VALUE', element_nullable=True)
            exdate_string = self.navigator.get_element_value(item_element.find('QVDELIN176'), 'VALUE', element_nullable=True)
            extime_string = self.navigator.get_element_value(item_element.find('QVDELIN177'), 'VALUE', element_nullable=True)
            indicationproofdate_string = self.navigator.get_element_value(item_element.find('QVDELIH180'), 'VALUE', element_nullable=True)
            indicationprooftime_string = self.navigator.get_element_value(item_element.find('QVDELIH181'), 'VALUE', element_nullable=True)
            indicationproof_string = self.navigator.get_element_value(item_element.find('QVDELIN179'), 'VALUE', element_nullable=True)

            indatetime = DateTimeParser.parse_datetime(indate_string, intime_string, 'QVDELIN174/QVDELIN175')
            exdatetime = DateTimeParser.parse_datetime(exdate_string, extime_string, 'QVDELIN176/QVDELIN177')
            indicationproof_datetime = DateTimeParser.parse_datetime(indicationproofdate_string, indicationprooftime_string, 'QVDELIH180/QVDELIH181', element_nullable=True)

            indicationproof = XMLDeserializerHelper.determine_yes_no_value(indicationproof_string)

            if not indatetime or not type_string:
                return None

            return ForeignObject(
                type_=type_string,
                datetime=indatetime,
                removal_datetime=exdatetime,
                indicationproof_datetime=indicationproof_datetime,
                indicationproof=indicationproof
            )
        except ValueError:
            pass
