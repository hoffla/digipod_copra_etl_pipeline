from dataclasses import dataclass
from typing import Optional
from datetime import datetime

from models.Preprocessing.DeserializedObjects.PeriOp.PeriOp import PerioOP
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper
from models.Preprocessing.Utils.DateTimeParser import DateTimeParser


@dataclass
class PerioOPDeserializer(BaseDeserializer):
    def deserialize(self) -> PerioOP:
        eeg = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN326'))
        regional_anaesthesia = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN327'))

        fasting_drink_time = self._parse_datetime('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN058', './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN059')
        fasting_food_time = self._parse_datetime('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN060', './/SUB_DOC/SUB_DOC_CONTENT/QVDELIN061')

        high_caloric_fluid = XMLDeserializerHelper.determine_yes_no_ka_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN398'))
        medication_administered = XMLDeserializerHelper.determine_yes_no_ka_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN062'))

        medication_reason = self._map_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN328'), {
            "A": "von Pat. abgelehnt",
            "N": "nicht angesetzt",
            "S": "Sonstiger Grund",
            "NULL": "k.A."
        })

        medication = self._map_value(self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN063'), {
            "1": "Midazolam",
            "2": "Clonidin",
            "3": "Ketanest"
        })

        medication_dose = self._parse_decimal_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN064')
        medication_time = self._parse_datetime('QVDELIN295', 'QVDELIN296')

        medication_unit = self._get_element_value('.//SUB_DOC/SUB_DOC_CONTENT/QVDELIN329')

        return PerioOP(
            eeg=eeg,
            regional_anaesthesia=regional_anaesthesia,
            fasting_drink_time=fasting_drink_time,
            fasting_food_time=fasting_food_time,
            high_caloric_fluid=high_caloric_fluid,
            medication_administered=medication_administered,
            medication_reason=medication_reason,
            medication=medication,
            medication_unit=medication_unit,
            medication_dose=medication_dose,
            medication_time=medication_time
        )

    def _map_value(self, value: Optional[str], mapping: dict) -> Optional[str]:
        return mapping.get(value.strip().upper()) if value else None

    def _parse_datetime(self, date_path: str, time_path: str) -> Optional[datetime]:
        date_value = self._get_element_value(date_path)
        time_value = self._get_element_value(time_path)
        return DateTimeParser.parse_datetime(date_value, time_value, f'{date_path} / {time_path}') if date_value and time_value else None

    def _parse_decimal_value(self, path: str) -> Optional[float]:
        value = self._get_element_value(path)
        try:
            return float(value) if value else None
        except ValueError:
            return None
