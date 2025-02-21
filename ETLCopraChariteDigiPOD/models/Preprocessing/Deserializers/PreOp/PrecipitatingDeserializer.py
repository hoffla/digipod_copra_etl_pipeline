from dataclasses import dataclass

from models.Preprocessing.DeserializedObjects.PreOp.PrecipitatingFactors import PrecipitatingFactors
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper


@dataclass
class PrecipitatingFactorsDeserializer(BaseDeserializer):
    def deserialize(self) -> PrecipitatingFactors:
        items = {
            'expected_op_duration': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN044',
            'abdominal_procedure': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN045',
            'intrathoracic_procedure': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN046',
            'major_surgery': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN047',
            'expected_severe_postoperative_pain': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN048',
            'contraindication': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN049',
            'expected_transfusion_of_blood_products': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN050',
        }

        for key, value in items.items():
            element_value = self._get_element_value(value)
            items[key] = XMLDeserializerHelper.determine_yes_no_value(element_value)

        return PrecipitatingFactors(
            expected_op_duration=items['expected_op_duration'],
            abdominal_procedure=items['abdominal_procedure'],
            intrathoracic_procedure=items['intrathoracic_procedure'],
            major_surgery=items['major_surgery'],
            expected_severe_postoperative_pain=items['expected_severe_postoperative_pain'],
            contraindication=items['contraindication'],
            expected_transfusion_of_blood_products=items['expected_transfusion_of_blood_products']
        )
