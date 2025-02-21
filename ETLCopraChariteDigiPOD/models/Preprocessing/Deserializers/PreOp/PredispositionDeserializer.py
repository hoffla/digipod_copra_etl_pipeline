from models.Preprocessing.DeserializedObjects.PreOp.PredispositionFactors import PredispositionFactors
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer


class PredispositionDeserializer(BaseDeserializer):
    def deserialize(self) -> PredispositionFactors:
        items = {
            'advanced_age': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN345',
            'chronic_pain': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN024',
            'anxiety_disorder': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN025',
            'asa_grade_3': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN031',
            'dementia': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN032',
            'electrolyte_disorder': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN033',
            'swallowing_disorder': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN035',
            'alcoholism': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN038',
            'anemia': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN039',
            'diabetes': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN040',
            'insulin_dependent': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN041',
            'parkinson': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN042',
            'sensory_deficit': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN051',
            'immunosuppression': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN348',
            'cardiac_disease': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN356',
            'vascular_disease': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN361',
            'stroke': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN362',
            'sleep_disorder': './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN364',
        }

        for key, value in items.items():
            new_value = self._get_element_value(value)
            items[key] = XMLDeserializerHelper.determine_yes_no_value(new_value)

        return PredispositionFactors(
            advanced_age=items['advanced_age'],
            chronic_pain=items['chronic_pain'],
            anxiety_disorder=items['anxiety_disorder'],
            asa_grade_3=items['asa_grade_3'],
            dementia=items['dementia'],
            electrolyte_disorder=items['electrolyte_disorder'],
            dysphagia=items['swallowing_disorder'],
            alcoholism=items['alcoholism'],
            anemia=items['anemia'],
            diabetes=items['diabetes'],
            insulin_dependent=items['insulin_dependent'],
            parkinson=items['parkinson'],
            sensory_deficit=items['sensory_deficit'],
            immunosuppression=items['immunosuppression'],
            cardiac_disease=items['cardiac_disease'],
            vascular_disease=items['vascular_disease'],
            stroke=items['stroke'],
            sleep_disorder=items['sleep_disorder']
        )



