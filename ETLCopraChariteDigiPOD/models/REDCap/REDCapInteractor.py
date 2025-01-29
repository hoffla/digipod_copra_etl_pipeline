import datetime
import difflib
import re
import requests
import os

from functools import wraps
from typing import Callable, Any
from dataclasses import dataclass, field

from models.PipelineStarter.event import post_event
from models.Utils.logger import get_logger

logger = get_logger(__name__)

checkBoxVars = ('angst_bewaltigung_typ', 'circrhy_wie', 'praemed_rf_praezi', 'praemed_rf_praedi')


def log_status_code(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
        if response.status_code == 200:
            args = re.sub(r"'token': '[A-Z0-9]+', ", "'token': 'ommited', ", str(args))
            logger.info(f"Query {args} was successfull!")
        else:
            args = re.sub(r"'token': '[A-Z0-9]+', ", "'token': 'ommited', ", str(args))
            logger.error(f"Error with query {args} - status code: {response.status_code}")
        return response

    return wrapper


def log_query(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        result, string_to_check, hit = func(*args, **kwargs)

        if hit == 2:
            logger.debug(f"Result '{result}' matches checked string: '{string_to_check}'!")
        elif hit == 1:
            logger.warning(f"Result '{result}' does not match checked string: '{string_to_check}'!")
        else:
            logger.error(f"No result was found! Critical error with '{string_to_check}'")

        return result, string_to_check, hit

    return wrapper


def remove_digits(text):
    pattern = r'\[\d+\]'
    cleaned_text = re.sub(pattern, '', text)
    return cleaned_text


@dataclass
class PayLoadGenerator:
    possibleKwargs: dict = field(init=True, repr=False)
    payload: dict = field(init=False, repr=False)

    def generatePayLoad(self, **kwargs):
        self.payload = dict()

        for key, value in kwargs.items():
            if isinstance(value, (list, tuple)):
                self.__addIterativeKey(key, value)
            elif value:
                self.__addKey(key, value)

        self.__checkKWArgs(**self.payload)

        return self.payload

    def __addKey(self, key, value):
        self.payload[key] = value

    def __addIterativeKey(self, key, values):
        baseNum = 0
        for value in values:
            newKey = key + f'[{baseNum}]'
            self.payload[newKey] = value
            baseNum += 1

    def __checkKWArgs(self, **kwargs):
        for key, value in kwargs.items():
            key = remove_digits(key)
            if key not in self.possibleKwargs:
                raise ValueError(f'Given key "{key}" is not in the available API Data Arguments')
            if self.possibleKwargs[key][0] != "" and value not in self.possibleKwargs[key]:
                raise ValueError(f'Given value "{value}" for key "{key}" is not in the available API Data Arguments')


@dataclass
class DataChecker:
    records: list
    events: list
    forms: list
    fields: list
    repeatFormsEvents: list
    dag: list

    def __post_init__(self):
        self.mappings = {
            'records': (self.__processIterable, self.records, True),
            'events': (self.__processIterable, self.events, False),
            'forms': (self.__processIterable, self.forms, False),
            'fields': (self.__processIterable, self.fields, False),
            'data': (self.__checkData, [], False),
            'record_id': (self.__processIterable, self.records, True),
            'redcap_data_access_group': (self.__processIterable, self.dag, False)
        }

    def checkData(self, kwargs) -> dict:
        for key, val in kwargs.items():
            func, reference_list, boolean = self.mappings.get(key)
            if callable(func):
                kwargs[key] = func(val, reference_list, boolean)

        return kwargs

    def __checkData(self, data: tuple or list, *_) -> list:
        '''
        Longitudinal: Enhält die Felder - Datensatz* (record*), Feldname (field_name), Wert (value), REDCap-Event-Name (redcap_event_name)

        patData = [{'record_id': 'Digi-TESTE-094', 'redcap_event_name': 'einschluss_arm_2', 'redcap_repeat_instrument': '', 'redcap_repeat_instance': '', 'redcap_data_access_group': 'charit', 'einschluss_datetime': '2024-06-11',
        'geburtstag': '01/04/1951', 'alter': '73', 'krankenkasse': '9', 'krankenkasse_sonstige': '', 'aufnahme_pat': '1', 'aufnahme_statt_pat': '', 'einschluss_complete': '2'}]

        '''
        for entry in data:
            for key, val in entry.items():
                transKey = self.__transformKey(key)
                func, reference_list, boolean = self.mappings.get(transKey)
                if callable(func):
                    newVal = func(val, reference_list, boolean)
                    self.__checkNewVal(key, newVal)
                    entry[key] = newVal

            if entry.get('redcap_repeat_instrument'):
                self.__checkPairRepeating(entry)

        return data

    def __transformKey(self, key):
        key = self.__checkAndconvertForOtherEventsKeyParams(key)
        transKey = key if key in self.mappings else 'fields'
        return transKey

    def __checkAndconvertForOtherEventsKeyParams(self, key) -> str:
        complete_pattern = re.compile(r'^(.*)_complete$|redcap_event_name')
        match = complete_pattern.match(key)
        if match:
            key = "events"
        return key

    def __processIterable(self, iterable: tuple or list, reference_list, boolean):
        processedElems = []
        for i in iterable:
            if i not in checkBoxVars:
                result, _, _ = self.checkAndmatch_strings(i, reference_list, boolean)
                if result:
                    processedElems.append(result)
            else:
                processedElems.append(i)

        return processedElems

    def __checkNewVal(self, key, newVal) -> None:
        if key in ('redcap_repeat_instrument', 'redcap_repeat_instance'):
            if not newVal:
                logger.error(f"The parameter {key} must have a proper value. Given value - {newVal}")
                raise ValueError(f"The parameter {key} must have a proper value. Given value - {newVal}")

    @log_query
    def checkAndmatch_strings(self, string_to_check, reference_list, exactMatch=False) -> tuple[str, str, int]:
        if string_to_check in reference_list:
            result, hit = string_to_check, 2
        else:
            closest_match = difflib.get_close_matches(string_to_check, reference_list, n=1, cutoff=0.6)
            if closest_match and not exactMatch:
                result, hit = closest_match[0], 1
            else:
                result, hit = '', 0

        return result, string_to_check, hit

    def __checkPairRepeating(self, entry):
        repeatingPair = (entry.get('redcap_event_name'), entry.get('redcap_repeat_instrument'))
        if repeatingPair not in self.repeatFormsEvents:
            logger.error(
                f"The pair of event '{repeatingPair[0]}' and form '{repeatingPair[1]}' does not exist in the current REDCap Instance")
            raise ValueError(
                f"The pair of event '{repeatingPair[0]}' and form '{repeatingPair[1]}' does not exist in the current REDCap Instance")


@dataclass
class QueryGenerator:
    payloadGen: PayLoadGenerator = field(init=True, repr=True)

    def getQuery(self, token, type: str, lastProcessing='', **kwargs):
        match type:
            case 'getData':
                defaults = {"action": "export", "content": "record", "type": "flat", "format": "json",
                            "returnFormat": "json", 'exportDataAccessGroups': 'true'}
                kwargs = self.__updateKwargs(defaults, kwargs, lastProcessing)
            case 'getData_records':
                defaults = {"action": "export", "content": "record", "type": "flat", "format": "json",
                            'events[0]': 'einschluss_arm_2', 'fields[0]': 'record_id', "returnFormat": "json",
                            'exportDataAccessGroups': 'true'}
                kwargs = self.__updateKwargs(defaults, kwargs)
            case 'getData_events':
                defaults = {"content": "event", "format": "json", "returnFormat": "json"}
                kwargs = self.__updateKwargs(defaults, kwargs)
            case 'getData_forms':
                defaults = {"content": "instrument", "format": "json", "returnFormat": "json"}
                kwargs = self.__updateKwargs(defaults, kwargs)
            case 'getData_repeatingFormsEvents':
                defaults = {"content": "repeatingFormsEvents", "format": "json", "returnFormat": "json"}
                kwargs = self.__updateKwargs(defaults, kwargs)
            case 'getData_fields':
                defaults = {"content": "exportFieldNames", "format": "json", "returnFormat": "json"}
                kwargs = self.__updateKwargs(defaults, kwargs)
            case 'getData_dag':
                defaults = {"content": "dag", "format": "json", "returnFormat": "json"}
                kwargs = self.__updateKwargs(defaults, kwargs)
            case 'setData':
                defaults = {"action": "import", "content": "record", "type": "flat", "format": "json",
                            "returnFormat": "json"}
                kwargs = self.__updateKwargs(defaults, kwargs)
            case _:
                logger.error(f"getQuery function called with unsupported 'type': {type}.")
                raise ValueError

        query = self.payloadGen.generatePayLoad(token=token, **kwargs)
        return query

    def __updateKwargs(self, defaults, kwargs, lastProcessing=''):
        if lastProcessing:
            kwargs.update({"dateRangeBegin": lastProcessing})

        for key, value in defaults.items():
            kwargs.setdefault(key, value)

        return kwargs


@dataclass
class REDCapDataProcessor:
    @classmethod
    def processRawData(cls, rawData: list[dict], type: str) -> list:
        keyword = cls.__identifyKeywordByType(type)
        data = cls.__processItems(rawData, keyword)
        return data

    @classmethod
    def __identifyKeywordByType(cls, type) -> str or tuple:
        types = {
            'getData_records': 'record_id',
            'getData_events': 'unique_event_name',
            'getData_forms': 'instrument_name',
            'getData_fields': 'export_field_name',
            'getData_repeatingFormsEvents': ('event_name', 'form_name'),
            'getData_dag': 'unique_group_name',
        }

        keyword = types.get(type)
        if keyword:
            return keyword

        logger.error(f"Given type '{type}' it's currently not implemented")
        raise KeyError(f"Given type '{type}' it's currently not implemented")

    @classmethod
    def __processItems(cls, rawData, keyword) -> list:
        processedItems = list()
        for entry in rawData:
            if isinstance(keyword, tuple):
                processedItem = (entry.get(keyword[0]), entry.get(keyword[1]))
            else:
                processedItem = entry.get(keyword)

            if processedItem:
                processedItems.append(processedItem)

        return processedItems


@dataclass
class REDCapInteractor:
    api: str = field(init=True, repr=True)
    token: str = field(init=True, repr=False)
    queryGen: QueryGenerator = field(init=True, repr=True)
    lastProcessing: str = field(init=False)

    def __post_init__(self):
        self.lastProcessingPath = os.path.join(os.getenv('BASEPATH'), os.getenv('PROJECTDIRECTORY'), 'Ressources',
                                               'lastProcessing.txt')
        try:
            with open(self.lastProcessingPath, 'r') as file:
                self.lastProcessing = file.read().strip()
                print("LAST PROCESSING: ", self.lastProcessing)
        except FileNotFoundError:
            self.lastProcessing = ''

    def interact(self, queryType, onlyNewItems=True, **kwargs):
        lastProcessing = self.lastProcessing if onlyNewItems else ''
        query = self.queryGen.getQuery(self.token, queryType, lastProcessing, **kwargs)
        data = self.__postRequest(query)
        return data.json()

    def getDatacheckerDependencies(self) -> list:
        depedencies = list()
        types = 'getData_records', 'getData_events', 'getData_forms', 'getData_fields', 'getData_repeatingFormsEvents', 'getData_dag'
        for type in types:
            rawDependency = self.interact(type, onlyNewItems=False)
            dependency = REDCapDataProcessor.processRawData(rawDependency, type)
            depedencies.append(dependency)

        return depedencies

    @log_status_code
    def __postRequest(self, query):
        response = requests.post(self.api, data=query)
        return response

    def setLastProcessing(self, processingDatetime: datetime.datetime):
        processingDatetimeString = processingDatetime.strftime('%Y-%m-%dT%H:%M:%S')

        if not post_event('getExceptionStatus'):
            with open(self.lastProcessingPath, 'w') as file:
                file.write(processingDatetimeString)
                self.lastProcessing = processingDatetimeString
        else:
            logger.warning(
                f'Last processing time was not updated to {processingDatetimeString} since an exception was raised during the processing of the data. \nLast processing time remains unchanged ({self.lastProcessing}).')


# def generatePayLoad(self, token: str, action: str ='export', records: list[str] = None, forms: list[str] = None,
#                    events: list[str] = None, fields: list[str] = None, **kwargs):


importPL = {
    "token": "",
    "content": "record",
    "format": "json",  # "csv", "json", "xml" [standard], "odm"
    "type": "flat",  # "eav"
    "overwriteBehavior": "normal",  # "overwrite"
    "forceAutoNumber": "false",  # "true"
    "data": """[{'record_id': 'Digi-CV-010', 'redcap_event_name': 'einschluss_arm_2', 'redcap_repeat_instrument': '', 'redcap_repeat_instance': '', 'redcap_data_access_group': 'charit', 'einschluss_datetime': '2024-06-11 11:42', 
        'geburtstag': '1951-04-01', 'alter': '73', 'krankenkasse': '9', 'krankenkasse_sonstige': '', 'aufnahme_pat': '1', 'aufnahme_statt_pat': '', 'einschluss_complete': '2'}]""",
    "dateFormat": "YMD",  # MDY 04/24/2024, DMY 24/04/2024, YMD [default] 2002-10-25
    "csvDelimiter": ",",
    "returnContent": "count",
    "returnFormat": "json"  # "csv", "json", "xml"
}

exportPL = {
    "token": "",
    "content": "record",
    "action": "export",
    "format": "json",  # "csv", "json", "xml" [standard], "odm"
    "type": "flat",  # "eav"
    "records[0]": "Digi-CV-001",
    "fields[0]": "record_id",
    "forms[0]": "einschluss",
    "events[0]": "einschluss_arm_2",
    "rawOrLabel": "raw",  # "raw" [default], "label" - (for 'csv' format 'flat' type only)
    "rawOrLabelHeaders": "raw",  # "raw" [default], "label" - (for 'csv' format 'flat' type only)
    "exportCheckboxLabel": "false",  # "true", "false" [default]
    "returnFormat": "json",
    "exportSurveyFields": "false",  # "true", "false" [default]
    "exportDataAccessGroups": "true",  # "true", "false" [default]
    "filterLogic": "",  # [age] > 30
    "dateRangeBegin": "",  # '2017-01-01 00:00:00'
    "dateRangeEnd": "",
    # '2017-01-01 00:00:00'  NOTE: The default format is Y-M-D (with Striche), while MDY and DMY values should always be formatted as M/D/Y or D/M/Y (with Schrägstriche), respectively.
    "csvDelimiter": ",",
    "decimalCharacter": ".",  # "," "."
    "exportBlankForGrayFormStatus": "false"  # "true", "false" [default]
}