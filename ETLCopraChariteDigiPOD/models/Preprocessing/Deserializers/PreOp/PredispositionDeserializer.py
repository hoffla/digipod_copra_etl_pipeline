from models.Preprocessing.DeserializedObjects.PreOp.PredispositionFactors import PredispositionFactors
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer


class PredispositionDeserializer(BaseDeserializer):
    def deserialize(self) -> PredispositionFactors:
        items = {
            'advanced_age': 'QVDELIN345',
            'chronic_pain': 'QVDELIN024',
            'anxiety_disorder': 'QVDELIN025',
            'asa_grade_3': 'QVDELIN031',
            'dementia': 'QVDELIN032',
            'electrolyte_disorder': 'QVDELIN033',
            'swallowing_disorder': 'QVDELIN035',
            'alcoholism': 'QVDELIN038',
            'anemia': 'QVDELIN039',
            'diabetes': 'QVDELIN040',
            'insulin_dependent': 'QVDELIN041',
            'parkinson': 'QVDELIN042',
            'sensory_deficit': 'QVDELIN051',
            'immunosuppression': 'QVDELIN348',
            'cardiac_disease': 'QVDELIN356',
            'vascular_disease': 'QVDELIN361',
            'stroke': 'QVDELIN362',
            'sleep_disorder': 'QVDELIN364',
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



