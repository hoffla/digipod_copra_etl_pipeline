import traceback
from dataclasses import dataclass

from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.DeserializedObjects.VisitMetadata import VisitMetadata
from models.Preprocessing.Utils.DateTimeParser import DateTimeParser
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper
from models.Utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class VisitMetadataDeserializer(BaseDeserializer):
    def deserialize(self) -> VisitMetadata:
        try:
            patnummer = self.navigator.get_element_value(self.navigator.find_element('.//MAIN_DOC/MAIN_DOC_METADATA/PATNR'), 'VALUE')
            fallnummer = self.navigator.get_element_value(self.navigator.find_element('.//EINWILLIGUNGSSTATUS/FALNR'), 'VALUE')
            op_ankernummer = self.navigator.get_element_value(self.navigator.find_element('.//EINWILLIGUNGSSTATUS/LNRLS'), 'VALUE')
            doknr = self.navigator.get_element_value(self.navigator.find_element('.//MAIN_DOC/MAIN_DOC_KEY/DOKNR'), 'VALUE')
            #doktl = self.navigator.get_element_value(self.navigator.find_element('.//SUB_DOC/SUB_DOC_KEY/DOKTL'), 'VALUE', element_nullable=True) TODO: existe esse tag?
            mitarbeiter = self.navigator.get_element_value(self.navigator.find_element('.//SUB_DOC/SUB_DOC_METADATA/MITARB'), 'VALUE')
            place = self.navigator.get_element_value(self.navigator.find_element('.//SUB_DOC/SUB_DOC_METADATA/ORGDO'), 'VALUE')
            typ = self.navigator.get_element_value(self.navigator.find_element('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN002'), 'VALUE')
            deleted_ = self.navigator.get_element_value(self.navigator.find_element('.//SUB_DOC/SUB_DOC_METADATA/LOEKZ'), 'VALUE')
            visitendatum_string = self.navigator.get_element_value(self.navigator.find_element('.//MAIN_DOC/MAIN_DOC_METADATA/DODAT'), 'VALUE')
            visitenzeit_string = self.navigator.get_element_value(self.navigator.find_element('.//MAIN_DOC/MAIN_DOC_METADATA/DOTIM'), 'VALUE')

            visiten_datetime = DateTimeParser.parse_datetime(visitendatum_string, visitenzeit_string, 'MAIN_DOC.MAIN_DOC_METADATA')

            is_deleted = XMLDeserializerHelper.determine_yes_no_value(deleted_)

            return VisitMetadata(
                patnummer=self._tryConvertString(patnummer),
                casenumber=self._tryConvertString(fallnummer),
                datetime=visiten_datetime,
                opAnkernum=self._tryConvertString(op_ankernummer),
                provider=mitarbeiter,
                place=place,
                doknr=self._tryConvertString(doknr),
                #doktl=self._tryConvertString(doktl),
                type_=typ,
                deleted=is_deleted
            )

        except Exception as err:
            tracebackString = "".join(traceback.format_exception(type(err), err, err.__traceback__))
            logger.error(f"Error deserializing VisitMetadata: {tracebackString}")

    @staticmethod
    def _tryConvertString(value):
        if isinstance(value, str) and value:
            return int(value)
        return value
