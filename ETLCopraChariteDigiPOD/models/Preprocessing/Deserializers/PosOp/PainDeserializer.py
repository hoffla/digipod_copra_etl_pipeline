import traceback
from dataclasses import dataclass
from typing import List, Callable
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.DeserializedObjects.PosOp.Pain import NRS, BPS, BPSNI, BESD, PainScore
from models.Preprocessing.Utils.DateTimeParser import DateTimeParser
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper
from datetime import datetime

from models.Utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class PainScoreDeserializer(BaseDeserializer):
    def deserialize(self) -> List[PainScore]:
        pain_scores = []
        items = self.navigator.find_elements('.//SUB_DOC_CONTENT/QVDELIN133/ITEM')

        deserializer_map: dict[str, Callable] = {
            'NRS': self._deserialize_nrs,
            'BPS': self._deserialize_bps,
            'BPSNI': self._deserialize_bpsni,
            'BESD': self._deserialize_besd,
        }

        for item in items:
            score_type = self.navigator.get_element_value(item.find('X00ELIN128'), 'VALUE')

            try:
                score_date = self.navigator.get_element_value(item.find('X00ELIN131'), 'VALUE')
                score_time = self.navigator.get_element_value(item.find('X00ELIN132'), 'VALUE')

                parsed_datetime = DateTimeParser.parse_datetime(score_date, score_time, 'X00ELIN131/X00ELIN132')

                score_sum_str = self._get_element_value('QVDELIN134')
                if not score_sum_str or not score_sum_str.isdigit() or not parsed_datetime:
                    logger.warning(f"Invalid score format for QVDELIN134: {score_sum_str}")
                    raise ValueError

                score_sum = int(score_sum_str)

                if score_type in deserializer_map:
                    deserialized_score = deserializer_map[score_type](item, parsed_datetime, score_sum)
                    pain_scores.append(deserialized_score)

            except Exception as err:
                tracebackString = "".join(traceback.format_exception(type(err), err, err.__traceback__))
                logger.debug(f"An error occurred while trying to process the following delirium score '{score_type}'. Error: {tracebackString}")

        return pain_scores

    def _deserialize_nrs(self, item, dt: datetime, score: int) -> NRS:
        tolerable = XMLDeserializerHelper.determine_yes_no_value(self.navigator.get_element_value(item.find('QVDELIN135'), 'VALUE'))
        condition = self.navigator.get_element_value(item.find('QVDELIN136'), 'VALUE')
        local = self.navigator.get_element_value(item.find('QVDELIN137'), 'VALUE')
        quality = self.navigator.get_element_value(item.find('QVDELIN138'), 'VALUE')

        return NRS(
            type_='NRS',
            datetime=dt,
            score=score,
            tolerable=tolerable,
            condition=condition,
            local=local,
            quality=quality
        )

    def _deserialize_bps(self, item, dt: datetime, score: int) -> BPS:
        face = int(self.navigator.get_element_value(item.find('QVDELIN140'), 'VALUE'))
        upper_ext = int(self.navigator.get_element_value(item.find('QVDELIN141'), 'VALUE'))
        adaptation_vent = int(self.navigator.get_element_value(item.find('QVDELIN142'), 'VALUE'))

        return BPS(
            type_='BPS',
            datetime=dt,
            score=score,
            face=face,
            upperExt=upper_ext,
            adaptationVent=adaptation_vent
        )

    def _deserialize_bpsni(self, item, dt: datetime, score: int) -> BPSNI:
        face = int(self.navigator.get_element_value(item.find('QVDELIN140'), 'VALUE'))
        upper_ext = int(self.navigator.get_element_value(item.find('QVDELIN141'), 'VALUE'))
        vocalisation = int(self.navigator.get_element_value(item.find('QVDELIN143'), 'VALUE'))

        return BPSNI(
            type_='BPSNI',
            datetime=dt,
            score=score,
            face=face,
            upperExt=upper_ext,
            vocalisation=vocalisation
        )

    def _deserialize_besd(self, item, dt: datetime, score: int) -> BESD:
        condition = self.navigator.get_element_value(item.find('QVDELIN453'), 'VALUE')
        breathing = int(self.navigator.get_element_value(item.find('QVDELIN454'), 'VALUE'))
        negative_vocalisation = int(self.navigator.get_element_value(item.find('QVDELIN455'), 'VALUE'))
        face = int(self.navigator.get_element_value(item.find('QVDELIN456'), 'VALUE'))
        body_language = int(self.navigator.get_element_value(item.find('QVDELIN457'), 'VALUE'))
        consolation = int(self.navigator.get_element_value(item.find('QVDELIN458'), 'VALUE'))

        return BESD(
            type_='BESD',
            datetime=dt,
            score=score,
            condition=condition,
            breathing=breathing,
            negativeVocalisation=negative_vocalisation,
            face=face,
            bodyLanguage=body_language,
            consolation=consolation
        )
