import pytz
from datetime import datetime
from models.PipelineStarter.Ressources.Timezone import timezone
from models.Utils.logger import get_logger

logger = get_logger(__name__)


class DateTimeParser:
    @staticmethod
    def contains_value(string):
        return string is not None and len(string.strip()) > 0

    @staticmethod
    def try_parse_datetime(date_str, time_str):
        """
        Parses date and time strings in the format 'yyyyMMdd' for date and 'HH:mm:ss' for time.
        Automatically applies Berlin timezone (CET/CEST).
        """
        berlin_tz = pytz.timezone(timezone)
        try:
            logger.debug(f"Attempting to parse date: {date_str} and time: {time_str}")
            clean_time_str = time_str.replace(":", "")
            naive_dt = datetime.strptime(f"{date_str} {clean_time_str}", "%Y%m%d %H%M%S")
            localized_dt = berlin_tz.localize(naive_dt, is_dst=False)
            logger.debug(f"Successfully parsed and localized datetime: {localized_dt.isoformat()}")
            return localized_dt
        except ValueError:
            return None

    @staticmethod
    def parse_datetime(date_str, time_str, context, element_nullable=False):
        """Parse datetime considering the expected formats and adds Berlin timezone."""
        if DateTimeParser.contains_value(date_str) and DateTimeParser.contains_value(time_str):
            parsed_date = DateTimeParser.try_parse_datetime(date_str, time_str)

            if not parsed_date:
                error_message = (
                    f"The text in {context} is not in the expected format 'yyyyMMdd/HHmmss': "
                    f"Date - {date_str}/ Time - {time_str}"
                )
                logger.error(error_message)
                raise ValueError(error_message)

            return parsed_date

        logger.warning(f"Date or time is missing for the following context: {context}. Date: {date_str}, Time: {time_str}")
        if element_nullable: return None
        else: raise ValueError
