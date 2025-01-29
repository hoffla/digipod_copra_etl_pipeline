import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from models.ConceptIDFinder.ConceptIDFinder import OMOPConceptIDMapper, DomainIDMapper, OMOPMapper
from models.PipelineStarter.ETLPipeline import OMOPExtractTransformLoad, ETLPipeline
from models.PipelineStarter.InitializeToolsBuilder.Utils import FileUpdater
from models.PipelineStarter.Ressources.PossibleKwargs import possibleKwargs
from models.PipelineStarter.Utils.MemoryMonitor import MemoryMonitor
from models.PipelineStarter.Utils.XMLDirectoryManager import XMLDirectoryManager
from models.PipelineStarter.event import post_event
from models.Preprocessing.DeserializerManager import DeserializerManager
from models.REDCap.REDCapInteractor import REDCapInteractor, PayLoadGenerator, QueryGenerator, DataChecker
from models.Utils.SQLInteractor import getOrCreateSQLEngine
from models.Utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ETLPipelineBuilder(ABC):
    @abstractmethod
    def getPipeline(self) -> ETLPipeline:
        pass

    def buildRedcapInteractor(self, *args):
        pass

    def buildDatachecker(self, *args):
        pass

    def buildSQLengine(self, *args):
        pass


@dataclass
class StandardETLPipelineBuilder(ETLPipelineBuilder):
    def getPipeline(self, debug=False) -> OMOPExtractTransformLoad:
        xml_dir = 'XML_DIR' if not debug else 'XML_TEST_DIR'
        api, token, baseDir, projectDir, xmlDir, memory_cap = [
            self.__get_env_vars(var) for var in ('API', 'API_TOKEN', 'BASEPATH', 'PROJECTDIRECTORY', xml_dir, 'MEMORY_CAP')
        ]

        deserializerManager = self.buildDeserializerManager()
        #redcap_interactor = self.buildRedcapInteractor(api, token)
        #sql_engine = getOrCreateSQLEngine()
        xmlDirectoryManager = self.buildXMLDirectoryManager(baseDir, projectDir, xmlDir)
        memoryMonitor = self.buildMemoryMonitor(memory_cap)

        omopExtractTransformLoad = OMOPExtractTransformLoad(deserializerManager, xmlDirectoryManager, memoryMonitor)

        return omopExtractTransformLoad

    @staticmethod
    def buildRedcapInteractor(api, token):
        payload_gen = PayLoadGenerator(possibleKwargs)
        query_gen = QueryGenerator(payload_gen)
        redcap_interactor = REDCapInteractor(api, token, query_gen)

        return redcap_interactor

    @staticmethod
    def buildDeserializerManager():
        deserializerManager = DeserializerManager()
        return deserializerManager

    @staticmethod
    def buildXMLDirectoryManager(baseDir, projectDir, xmlDir):
        xmlBaseDir = Path(baseDir, projectDir, xmlDir)
        xmlProcDir = xmlBaseDir / "processed"
        xmlErrDesDir = xmlBaseDir / "error" / "deserialization"
        xmlErrDfDir = xmlBaseDir / "error" / "df_processing"
        xmlQuarDir = xmlBaseDir / "quarantine"
        xmlDirectoryManager = XMLDirectoryManager(xmlBaseDir, xmlProcDir, xmlErrDesDir, xmlErrDfDir, xmlQuarDir)
        return xmlDirectoryManager

    @staticmethod
    def buildMemoryMonitor(memoryThreshold):
        memoryThreshold = float(memoryThreshold) if isinstance(memoryThreshold, str) else memoryThreshold
        memoryMonitor = MemoryMonitor(memoryThreshold)
        return memoryMonitor

    @staticmethod
    def buildDatachecker(records, events, forms, fields, repeat_forms, dag):
        data_checker = DataChecker(records, events, forms, fields, repeat_forms, dag)

        return data_checker

    @staticmethod
    def __get_env_vars(var) -> str:
        env_var = os.getenv(var)
        if not env_var:
            logger.critical(f"The environmental variable {var} could not be retrieved!")
            raise RuntimeError
        return env_var


@dataclass
class OMOPBuilder(ABC):
    @abstractmethod
    def getOMOPmapper(self):
        pass

    def buildConceptIDMapper(self):
        pass

    def buildDomainIDMapper(self):
        pass


@dataclass
class StandardOMOPMapperBuilder:
    base_path: str = field(init=False)
    fileUpdater: FileUpdater = field(init=False, default_factory=FileUpdater)

    def __post_init__(self):
        self.basePath = os.path.join(os.getenv('BASEPATH'), os.getenv('PROJECTDIRECTORY'), os.getenv('RESSOURCES_DIR'))

    def getOMOPmapper(self):
        conceptIDMapper = self.buildConceptIDMapper()
        domainIDMapper = self.buildDomainIDMapper()

        return OMOPMapper(conceptIDMapper, domainIDMapper)

    def buildConceptIDMapper(self):
        dfs = []
        sqlEngine = getOrCreateSQLEngine()

        for file_name in ('CONCEPT', 'CONCEPT_RELATIONSHIP', 'SOURCE_TO_CONCEPT_MAP', 'LOCAL_TO_LOCAL_MAP'):
            if file_name in ('CONCEPT', 'CONCEPT_RELATIONSHIP'):
                kwargs = self.__getKwargs(file_name)
                dfs.append(post_event('importTable', file_name.lower(), sqlEngine, **kwargs))
            else:
                dfs.append(self.__read_parquet(os.path.join(self.basePath, file_name + '.parquet')))

        return OMOPConceptIDMapper(*dfs)

    def buildDomainIDMapper(self):
        domainIDmap = self.__read_parquet(os.path.join(self.basePath, 'DomainIDMap' + '.parquet'))
        return DomainIDMapper(domainIDmap)

    def __read_parquet(self, file_path) -> pd.DataFrame:
        self.fileUpdater.updateFileIfNecessary(file_path)
        return pd.read_parquet(file_path)

    @staticmethod
    def __getKwargs(file_name) -> dict:
        if file_name == 'CONCEPT_RELATIONSHIP':
            return {
                'where': [('relationship_id', '=', 'Maps to')],
                'columns': ['concept_id_1', 'concept_id_2', 'relationship_id', 'invalid_reason'],
                'cast_columns': {'concept_id_1': 'VARCHAR'}
                    }
        elif file_name == 'CONCEPT':
            return {
                'columns': ['concept_id', 'concept_name', 'domain_id ', 'vocabulary_id', 'concept_class_id', 'concept_code'],
                #'cast_columns': {'concept_id': 'VARCHAR'}
            }
        return {}
        
        

