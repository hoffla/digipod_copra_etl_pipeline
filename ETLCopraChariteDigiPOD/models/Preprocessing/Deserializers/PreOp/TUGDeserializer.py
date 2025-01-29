from dataclasses import dataclass
from typing import Optional

from models.Preprocessing.DeserializedObjects.PreOp.TUG import TimedUpAndGoScore
from models.Preprocessing.Deserializers.BaseDeserializer import BaseDeserializer
from models.Preprocessing.Utils.DeserializerHelper import XMLDeserializerHelper
import decimal
import locale


@dataclass
class TimedUpAndGoScoreDeserializer(BaseDeserializer):
    def deserialize(self) -> TimedUpAndGoScore:
        value = self._parse_decimal_value('QVDELIN037')
        not_collected = XMLDeserializerHelper.determine_yes_no_value(self._get_element_value('QVDELIN417'))

        return TimedUpAndGoScore(
            value=value,
            not_collected=not_collected
        )

    def _parse_decimal_value(self, path: str) -> Optional[float]:
        value = self._get_element_value(path)
        try:
            if value:
                locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
                return float(decimal.Decimal(value))
        except decimal.InvalidOperation:
            return None
        return None
