import traceback
from abc import abstractmethod, ABC
from dataclasses import dataclass, field
from typing import List

import pandas as pd

from models.PipelineStarter.event import post_event
from models.Processing.DataProcessor import PipelineManager

from models.Utils.logger import get_logger

logger = get_logger(__name__)


class ETLPipeline(ABC):
    @abstractmethod
    def startPipeline(self) -> None:
        pass


@dataclass
class OMOPExtractTransformLoad:
    deserializerManager: object
    xmlDirectoryManager: object
    memoryMonitor: object

    pipelineManager: PipelineManager = field(init=False, default_factory=PipelineManager)

    usedFiles: set = field(init=False)
    errorFiles: set = field(init=False)

    def startPipeline(self, include_processed=False):
        digi_xml_files = self.__get_and_mark_xmlFiles(include_processed=include_processed)

        while True:
            if len(digi_xml_files) == 0:
                break
            deserializedVisits, digi_xml_files = self.__deserialize(digi_xml_files)
            post_event('setExceptionStatus', 'newException', False)
            self.__updateDataFrames(deserializedVisits)

        self.__tryProcessErrorFiles()

    def check_quarantine_files(self):
        self.xmlDirectoryManager.cleanOldQuarantineFiles()
        if post_event('checkChangesInCaseMappings'):
            digi_xml_files = self.__get_and_mark_xmlFiles(from_directory="quarantine", to_directory="unprocessed")
            self.xmlDirectoryManager.mark_as(digi_xml_files, "unprocessed")

    def __get_and_mark_xmlFiles(self, from_directory="unprocessed", to_directory="quarantine", include_processed=False) -> List[str]:
        xml_files = self.xmlDirectoryManager.get_list_files(from_directory, include_processed)

        if xml_files:
           digi_xml_files, possibly_digi_xml_files, no_digi_xml_files = post_event('filterXMLFiles', xml_files)
           self.xmlDirectoryManager.mark_as(possibly_digi_xml_files, to_directory)
           self.xmlDirectoryManager.delete_files(no_digi_xml_files)
           return digi_xml_files
        return []

    def __deserialize(self, xml_files) -> tuple[List, set]:
        deserializedVisits = list()
        self.usedFiles = set()
        self.errorFiles = set()

        for xml_file in xml_files:
            if self.memoryMonitor.is_memory_critical() and len(deserializedVisits) > 50:
                break

            deserializedVisit = self.__deserializedSingleVisit(xml_file)
            deserializedVisits = self.__handleDeserializedFile(deserializedVisits, deserializedVisit, xml_file)

        self.memoryMonitor.log_memory_usage()

        unusedFiles = set(xml_files)
        unusedFiles.difference_update(self.errorFiles)
        unusedFiles.difference_update(self.usedFiles)

        return deserializedVisits, unusedFiles

    def __handleDeserializedFile(self, deserializedVisits, deserializedVisit, xml_file):
        if deserializedVisit:
            deserializedVisits.append(deserializedVisit)
            self.usedFiles.add(xml_file)
        else:
            self.xmlDirectoryManager.mark_as(xml_file, "error_deserialization")
            self.errorFiles.add(xml_file)
        return deserializedVisits

    def __deserializedSingleVisit(self, xml_file):
        try:
            deserializedVisit = self.deserializerManager.deserialize(xml_file)
            return deserializedVisit
        except Exception as err:
            tracebackString = "".join(traceback.format_exception(type(err), err, err.__traceback__))
            logger.error(f'Error during deserialization of Visit. XML File: "{xml_file}". Error: {tracebackString}')
            post_event('sendEmail', f'Visit Deserialization Error', tracebackString)

    def __updateDataFrames(self, deserializedVisits):
        tables = post_event('getOMOPTableNames')

        rawData = self.__getRawDataFrame(deserializedVisits)
        for tableName in tables:
            try:
                dependencies = self.__getDependeciesForTable(rawData, tableName)
                processedTable = self.pipelineManager.processData(dependencies, tableName)
                post_event('updateTable', processedTable, tableName)
            except Exception as err:
                self.__handleException(err, tableName, dependencies)

        toDir = "processed" if not post_event('getExceptionStatus', 'newException') else "error_df_processing"
        self.xmlDirectoryManager.mark_as(self.usedFiles, toDir)

    @staticmethod
    def __getRawDataFrame(deserializedVisits) -> dict:
        rawData = dict()

        for deserializedVisit in deserializedVisits:
            visit_data = deserializedVisit.to_dict()
            for key, df in visit_data.items():
                if key in rawData:
                    rawData[key] = pd.concat([rawData[key], df], ignore_index=True)
                else:
                    rawData[key] = df

        return rawData

    @staticmethod
    def __getDependeciesForTable(rawData, tableName) -> dict:
        tableKeys = post_event('getTableDependecies', tableName)
        tableDependencies = {key: rawData[key] for key in tableKeys if key in rawData}
        return tableDependencies

    @staticmethod
    def __handleException(err, tableName, dependencies):
        post_event('setExceptionStatus', tableName, True)
        post_event('saveErrorDataframe', tableName, dependencies)
        tracebackString = "".join(traceback.format_exception(type(err), err, err.__traceback__))
        logger.error(tracebackString)
        post_event('sendEmail', f'Pipeline Error - Table {tableName}', tracebackString)

    def __tryProcessErrorFiles(self):
        tables = post_event('getOMOPTableNames')

        for tableName in tables:
            try:
                if post_event('checkChangesInScript', tableName) and post_event('getExceptionStatus', tableName):
                    logger.info(f'Attempting to process error files for table "{tableName}"')
                    errDependencies = post_event('loadErrorDataframe', tableName)
                    processedTable = self.pipelineManager.processData(errDependencies, tableName)
                    post_event('setExceptionStatus', tableName, False)
                    post_event('updateTable', processedTable, tableName)
            except Exception as err:
                self.__handleException(err, tableName, errDependencies)
            
            if not post_event('getExceptionStatus', tableName):
                post_event('deleteErrorDataframe', tableName)
