import re
import traceback

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.DeserializedObjects.PosOp.DeliriumScores import DeliriumScore, RassScore, GCSScore, ICDSCScore, \
    NuDescScore, DOSScore, DDS8Score, CamIcuScore
from models.Preprocessing.Ressources.Ressources import RASS_MAPPING

from models.Preprocessing.Utils.DateTimeParser import DateTimeParser
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper
from models.Utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class DeliriumScoreDeserializer(BaseDeserializer):
    item_element: object = None

    def deserialize(self) -> List[DeliriumScore]:
        delirium_scores = []
        items = self.navigator.find_elements('.//SUB_DOC_CONTENT/QVDELIN071/ITEM')

        for item in items:
            score_type_value = self.navigator.get_element_value(item.find('QVDELIN072'), 'VALUE')
            score_deserializer = DElIRIUM_SCORE_MAPPING.get(score_type_value)(self.navigator, item_element=item)

            try:
                if score_deserializer:
                    score = score_deserializer._deserialize()
                    print("Score", score)
                    delirium_scores.append(score)
            except Exception as err:
                tracebackString = "".join(traceback.format_exception(type(err), err, err.__traceback__))
                logger.debug(f"An error occurred while trying to process the following delirium score '{score_deserializer}'. Error: {tracebackString}")

        return delirium_scores

    def _deserialize_delirium_score(self) -> tuple[str, [int, None]]:
        score_text = self.navigator.get_element_value(self.item_element.find('QVDELIN073'), 'VALUE', element_nullable=False)
        score = self.navigator.get_element_value(self.item_element.find('QVDELIN325'), 'VALUE', element_nullable=True)

        delirium_status = "Unknown"
        delirium_score = None

        if score is not None:
            try:
                match = re.search('[\d]+', score)
                delirium_score = int(match[0])
            except TypeError:
                logger.error(f'Class "{self.__class__.__name__}" could not corretly parse the value "{score}" into a integer score')

        if score_text and isinstance(delirium_score, int):
            delirium_status = self._process_delirium_status(score_text, delirium_score)

        return delirium_status, delirium_score

    def _process_delirium_status(self, score_text: str, score: int) -> str:
        pass

    @staticmethod
    def _process_delirium_status_from_text(value: str) -> str:
        delirium_patterns = {
            "Subsyndromales Delir": re.compile(r"\bSubsyndromales Delir\b", re.IGNORECASE),
            "Delir Wahrscheinlich": re.compile(r"\bDelir\s+Wahrscheinlich\b", re.IGNORECASE),
            "Kein Delir": re.compile(r"\bKein\s+Delir\b", re.IGNORECASE),
            "Delir": re.compile(r"\bDelir\b", re.IGNORECASE),
            "Unmöglich": re.compile(r"\bUnmöglich\b", re.IGNORECASE)
        }

        for status, pattern in delirium_patterns.items():
            if pattern.search(value):
                logger.debug(f'Score text "{status}" extracted from "{value}".')
                return status

        logger.debug(f'Exctraction from "{value}" was not successfull!')
        return "Unknown"

    def _parse_datetime_value(self) -> Optional[datetime]:
        date_string = self.navigator.get_element_value(self.item_element.find('QVDELIN074'), 'VALUE')
        time_string = self.navigator.get_element_value(self.item_element.find('QVDELIN075'), 'VALUE')

        score_datetime = DateTimeParser.parse_datetime(date_string, time_string, 'QVDELIN074/QVDELIN075')

        return score_datetime


@dataclass
class RassScoreDeserializer(DeliriumScoreDeserializer):
    def _deserialize(self) -> RassScore:
        rass_sum_string = self.navigator.get_element_value(self.item_element.find('QVDELIN094'), 'VALUE')

        score = int(rass_sum_string)
        score_datetime = self._parse_datetime_value()
        score_text = self.get_rass_description(score)

        return RassScore(
            type_="RASS",
            datetime=score_datetime,
            score=score,
            score_text=score_text
        )

    @staticmethod
    def get_rass_description(score) -> str or None:
        if score is not None:
            return RASS_MAPPING.get(score, None)
        return None

    def _process_delirium_status(self, score_text: str, score: str) -> str:
        return score_text


@dataclass
class GCSScoreDeserializer(DeliriumScoreDeserializer):
    def _deserialize(self) -> GCSScore:
        items = {
            'eye_reaction': 'QVDELIN095', 'voice_reaction': 'QVDELIN096', 'motor_reaction': 'QVDELIN097',
        }

        for key, value in items.items():
            newValue = self.navigator.get_element_value(self.item_element.find(value), 'VALUE')
            items[key] = int(newValue)

        score_datetime = self._parse_datetime_value()
        score_text, score = self._deserialize_delirium_score()

        return GCSScore(
            type_="GCS",
            datetime=score_datetime,
            score=score,
            score_text=score_text,
            eye_reaction=items['eye_reaction'],
            voice_reaction=items['voice_reaction'],
            motor_reaction=items['motor_reaction']
        )

    def _process_delirium_status(self, score_text: str, score: int) -> str:
        return score_text


@dataclass
class ICDSCScoreDeserializer(DeliriumScoreDeserializer):
    def _deserialize(self) -> ICDSCScore:
        items = {
            'modi_consc': 'QVDELIN098', 'inattention': 'QVDELIN099', 'desorient': 'QVDELIN100', 'hallu': 'QVDELIN101',
            'psychomotor': 'QVDELIN102', 'inap_comm': 'QVDELIN103', 'circ_rhythmus_disfunc': 'QVDELIN104', 'oscill_sympt': 'QVDELIN105',
        }

        for key, value in items.items():
            newValue = self.navigator.get_element_value(self.item_element.find(value), 'VALUE')
            items[key] = int(newValue)

        score_datetime = self._parse_datetime_value()
        score_text, score = self._deserialize_delirium_score()

        return ICDSCScore(
            type_="ICDSC",
            datetime=score_datetime,
            score=score,
            score_text=score_text,
            modi_conscientiousness=items['modi_consc'],
            inattention=items['inattention'],
            desorientation=items['desorient'],
            hallucinations=items['hallu'],
            psychomotor=items['psychomotor'],
            inappropriate_communication=items['inap_comm'],
            circ_rhythmus_disfunc=items['circ_rhythmus_disfunc'],
            oscillatory_symptomatic=items['oscill_sympt']
        )

    def _process_delirium_status(self, score_text: str, score: int) -> str:
        return self._process_delirium_status_from_score(score)

    def _process_delirium_status_from_score(self, score: int) -> str:
        if score == 0:
            return 'Kein Delir'
        elif 0 < score < 4:
            return 'Subsyndromales Delir'
        elif score >= 4:
            return 'Delir'
        else:
            logger.error(f'Class "{self.__class__.__name__}" received an unexpected value "{score}"')
            raise RuntimeError


@dataclass
class NuDescScoreDeserializer(DeliriumScoreDeserializer):
    def _deserialize(self) -> NuDescScore:
        items = {
            'desorient': 'QVDELIN065', 'inap_behavior': 'QVDELIN067', 'inap_commu': 'QVDELIN068',
            'hallu': 'QVDELIN070', 'psychomotor': 'QVDELIN081',
        }

        for key, value in items.items():
            newValue = self.navigator.get_element_value(self.item_element.find(value), 'VALUE', element_nullable=True)
            items[key] = int(newValue) if newValue else 0

        score_datetime = self._parse_datetime_value()
        score_text, score = self._deserialize_delirium_score()

        return NuDescScore(
            type_="Nu-DESC",
            datetime=score_datetime,
            score=score,
            score_text=score_text,
            desorientation=items['desorient'],
            inappropriate_behavior=items['inap_behavior'],
            inappropriate_communication=items['inap_commu'],
            hallucinations=items['hallu'],
            psychomotor_retardation=items['psychomotor']
        )

    def _process_delirium_status(self, score_text: str, score: str) -> str:
        return self._process_delirium_status_from_score(score)

    def _process_delirium_status_from_score(self, score: int) -> str:
        if 0 < score < 2:
            return 'Kein Delir'
        elif score >= 2:
            return 'Delir wahrscheinlich'
        else:
            logger.error(f'Class "{self.__class__.__name__}" received an unexpected value "{score}"')
            raise RuntimeError


@dataclass
class DOSDeserializer(DeliriumScoreDeserializer):
    def _deserialize(self) -> DOSScore:
        items = {
            'sleep': 'QVDELIN114', 'distraction': 'QVDELIN115', 'attention': 'QVDELIN116', 'question': 'QVDELIN117',
            'response': 'QVDELIN118', 'retardation': 'QVDELIN119', 'orientation': 'QVDELIN120', 'daytime': 'QVDELIN121',
            'memory': 'QVDELIN122', 'restlessness': 'QVDELIN123', 'removal_foreign': 'QVDELIN124', 'emotion': 'QVDELIN125', 'hallucination': 'QVDELIN126',
        }

        for key, value in items.items():
            newValue = self.navigator.get_element_value(self.item_element.find(value), 'VALUE', element_nullable=True)
            items[key] = XMLDeserializerHelper.determine_yes_no_ka_value_as_int(newValue)

        score_datetime = self._parse_datetime_value()
        score_text, score = self._deserialize_delirium_score()

        return DOSScore(
            type_="DOS",
            datetime=score_datetime,
            score=score,
            score_text=score_text,

            sleep=items['sleep'],
            distraction=items['distraction'],
            attention=items['attention'],
            question=items['question'],
            response=items['response'],
            retardation=items['retardation'],
            orientation=items['orientation'],
            daytime=items['daytime'],
            memory=items['memory'],
            restlessness=items['restlessness'],
            removalForeign=items['removal_foreign'],
            emotion=items['emotion'],
            hallucination=items['hallucination'],
        )

    def _process_delirium_status(self, score_text: str, score: int) -> str:
        return self._process_delirium_status_from_score(score)

    def _process_delirium_status_from_score(self, score: int) -> str:
        if score == 0:
            return 'Kein Delir'
        elif score >= 3:
            return 'Delir wahrscheinlich'
        else:
            logger.error(f'Class "{self.__class__.__name__}" received an unexpected value "{score}"')
            raise RuntimeError


@dataclass
class DDS8ScoreDeserializer(DeliriumScoreDeserializer):
    def _deserialize(self) -> DDS8Score:
        items = {
            'orientation': 'QVDELIN106', 'hallucination': 'QVDELIN107', 'agitation': 'QVDELIN108',
            'fear': 'QVDELIN109', 'myoclonia': 'QVDELIN110', 'paroxysmal_sweating': 'QVDELIN111',
            'sleep_disorder': 'QVDELIN112', 'tremor': 'QVDELIN113'
        }

        for key, value in items.items():
            newValue = self.navigator.get_element_value(self.item_element.find(value), 'VALUE')
            items[key] = int(newValue)

        score_datetime = self._parse_datetime_value()
        score_text, score = self._deserialize_delirium_score()

        return DDS8Score(
            type_="DDS",
            datetime=score_datetime,
            score=score,
            score_text=score_text,
            orientation=items["orientation"],
            hallucination=items["hallucination"],
            agitation=items["agitation"],
            fear=items["fear"],
            myoclonia=items["myoclonia"],
            paroxysmal_sweating=items["paroxysmal_sweating"],
            sleep_disorder=items["sleep_disorder"],
            tremor=items["tremor"]
        )

    def _process_delirium_status(self, score_text: str, score: int) -> str:
        return self._process_delirium_status_from_score(score)

    def _process_delirium_status_from_score(self, score: int) -> str:
        if score == 0:
            return 'Kein Delir'
        elif 0 < score < 4:
            return 'Subsyndromales Delir'
        elif score >= 4:
            return 'Delir'
        else:
            logger.error(f'Class "{self.__class__.__name__}" received an unexpected value "{score}"')
            raise RuntimeError


@dataclass
class CamIcuScoreDeserializer(DeliriumScoreDeserializer):
    def _deserialize(self) -> CamIcuScore:
        items = {
            "not_possible": ('QVDELIN084', XMLDeserializerHelper.determine_yes_no_value, None),
            "reason_not_possible": ('QVDELIN085', None, None),
            "feature1a": ('QVDELIN086', XMLDeserializerHelper.determine_yes_no_with_null_value, None),
            "feature1b": ('QVDELIN087', XMLDeserializerHelper.determine_yes_no_with_null_value, None),
            "feature2a": ('QVDELIN088', self.__parse_integer, (0, 10)),
            "feature2b": ('QVDELIN089', self.__parse_integer, (0, 8)),
            "feature3": ('QVDELIN090', self.__parse_integer, (-5, 4)),
            "feature4a": ('QVDELIN091', self.__parse_integer, (0, 4)),
            "feature4b": ('QVDELIN092', self.__parse_integer, (0, 1))
        }

        for key, values in items.items():
            value, func, args = values
            newValue = self.navigator.get_element_value(self.item_element.find(value), 'VALUE', element_nullable=True)

            if func and not args:
                newValue = func(newValue)
            elif func and args:
                newValue = func(newValue, args)

            items[key] = newValue

        score_datetime = self._parse_datetime_value()
        score_text, score = self._deserialize_delirium_score()

        return CamIcuScore(
            type_="CAM-ICU",
            datetime=score_datetime,
            score=score,
            score_text=score_text,
            notPossible=items["not_possible"],
            reasonNotPossible=items["reason_not_possible"],
            feature1a=items["feature1a"],
            feature1b=items["feature1b"],
            feature2a=items["feature2a"],
            feature2b=items["feature2b"],
            feature3=items["feature3"],
            feature4a=items["feature4a"],
            feature4b=items["feature4b"],
        )

    @staticmethod
    def __parse_integer(value: str, bounds: tuple) -> int or None:
        if value is None or not value.strip():
            return None

        try:
            intValue = int(value)
            lowerBound, upperBound = bounds
            if lowerBound <= intValue <= upperBound:
                return intValue
            logger.warning(f"The value '{value}' of CAM-ICU score is outside the required bounds (Lower Bound: {lowerBound} / Upper Bound: {upperBound}).")
            raise ValueError
        except ValueError:
            raise ValueError(f"The text is not in the expected 'integer' format: '{value}'")

    def _process_delirium_status(self, score_text: str, score: int) -> str:
        return self._process_delirium_status_from_text(score_text)


DElIRIUM_SCORE_MAPPING = {
    "1": NuDescScoreDeserializer,
    "2": CamIcuScoreDeserializer,
    "3": RassScoreDeserializer,
    "4": GCSScoreDeserializer,
    "5": ICDSCScoreDeserializer,
    "6": DDS8ScoreDeserializer,
    "7": DOSDeserializer,
}
