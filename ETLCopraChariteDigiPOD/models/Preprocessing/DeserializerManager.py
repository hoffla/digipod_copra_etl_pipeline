import os
from dataclasses import dataclass, field

from models.Preprocessing.DeserializedObjects.Visit import Visit
from models.Preprocessing.Deserializers.PeriOp.PeriOpDeserializer import PerioOPDeserializer
from models.Preprocessing.Deserializers.PosOp.CognitionDeserializer import CognitionDeserializer
from models.Preprocessing.Deserializers.PosOp.DeliriumScoreDeserializer import DeliriumScoreDeserializer
from models.Preprocessing.Deserializers.PosOp.FearAnxietyDeserializer import AnxietyDeserializer
from models.Preprocessing.Deserializers.PosOp.ForeignObjectsDeserializer import ForeignObjectsDeserializer
from models.Preprocessing.Deserializers.PosOp.MobilizationDeserializer import MobilizationDeserializer
from models.Preprocessing.Deserializers.PosOp.NutritionDeserializer import NutritionDeserializer, DysphagiaDeserializer, MouthHygieneDeserializer
from models.Preprocessing.Deserializers.PosOp.PainDeserializer import PainScoreDeserializer
from models.Preprocessing.Deserializers.PreOp.AUDITCDeserializer import AlkoholkonsumScoreDeserializer
from models.Preprocessing.Deserializers.PreOp.AdmissionStatusDeserializer import AdmissionStatusDeserializer
from models.Preprocessing.Deserializers.PreOp.DepressionDeserializer import DepressionScoreDeserializer
from models.Preprocessing.Deserializers.PreOp.FallRiskDeserializer import FallRiskDeserializer
from models.Preprocessing.Deserializers.PreOp.FrailtyDeserializer import FrailtyScoreDeserializer
from models.Preprocessing.Deserializers.PreOp.MiniCogDeserializer import MiniCogDeserializer
from models.Preprocessing.Deserializers.PreOp.NicotinDeserializer import NikotinkonsumScoreDeserializer
from models.Preprocessing.Deserializers.PreOp.PolypharmacyDeserializer import PolypharmazieScoreDeserializer
from models.Preprocessing.Deserializers.PreOp.PrecipitatingDeserializer import PrecipitatingFactorsDeserializer
from models.Preprocessing.Deserializers.PreOp.PredispositionDeserializer import PredispositionDeserializer
from models.Preprocessing.Deserializers.PreOp.SocialRiskDeserializer import SozialeSituationScoreDeserializer
from models.Preprocessing.Deserializers.PreOp.TUGDeserializer import TimedUpAndGoScoreDeserializer
from models.Preprocessing.Deserializers.VisitMetadataDeserializer import VisitMetadataDeserializer
from models.Preprocessing.Utils.XMLNavigator import XMLNavigator


deserializers = {
    'POST': [
        DeliriumScoreDeserializer, AnxietyDeserializer, CognitionDeserializer, 
        ForeignObjectsDeserializer, MobilizationDeserializer, PainScoreDeserializer, 
        NutritionDeserializer, DysphagiaDeserializer, MouthHygieneDeserializer
    ],
    'BASE': [
        AdmissionStatusDeserializer, AlkoholkonsumScoreDeserializer, DepressionScoreDeserializer,
        FallRiskDeserializer, FrailtyScoreDeserializer, MiniCogDeserializer, DeliriumScoreDeserializer,
        NikotinkonsumScoreDeserializer, PolypharmazieScoreDeserializer, PrecipitatingFactorsDeserializer,
        PredispositionDeserializer, SozialeSituationScoreDeserializer, TimedUpAndGoScoreDeserializer
    ],
    'PERI': [
        PerioOPDeserializer
    ]
}


@dataclass
class DeserializerManager:
    navigator: XMLNavigator = field(init=False)

    def __deserialize_with(self, deserializer_class):
        deserializer = deserializer_class(self.navigator)
        deserialized_obj = deserializer.deserialize()
        return deserialized_obj

    def deserialize(self, xml_file) -> object:
        self.navigator = XMLNavigator(xml_file)

        deserialized_objs = self.__instantiateVisitDepedencies(xml_file)

        for deserializer_class in self.deserializers:
            deserialized_obj = self.__deserialize_with(deserializer_class)
            if isinstance(deserialized_obj, list) and deserialized_obj:
                deserialized_objs[deserialized_obj[0].name] = deserialized_obj

            elif not isinstance(deserialized_obj, list) and deserialized_obj.isValidInformation:
                deserialized_objs[deserialized_obj.name] = deserialized_obj

        visit = Visit(deserialized_objs)

        return visit

    def __instantiateVisitDepedencies(self, xml_file) -> dict:
        visitDepedencies = dict()
        visitMetadata = self.__deserializeVisitMetadata(xml_file)
        visitDepedencies[visitMetadata.name] = visitMetadata
        return visitDepedencies

    def __deserializeVisitMetadata(self, xml_file):
        visitMetadata = self.__deserialize_with(VisitMetadataDeserializer)
        visitMetadata.__setattr__('file_id', os.path.basename(xml_file))
        visitType = visitMetadata.get_element('type_')
        self.deserializers = deserializers.get(visitType)
        return visitMetadata
