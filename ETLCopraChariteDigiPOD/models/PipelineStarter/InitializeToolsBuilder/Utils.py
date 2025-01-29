import json, os, math
import pandas as pd

from dotenv import load_dotenv
from dataclasses import dataclass, field

from models.Utils.logger import get_logger

logger = get_logger(__name__)


load_dotenv()


@dataclass
class TimestampAnalizer:
    timestamps: dict = field(init=False)

    def __post_init__(self):
        self.file_path = os.path.join(os.getenv('BASEPATH'), os.getenv('PROJECTDIRECTORY'), os.getenv('RESSOURCES_DIR'), 'timestamps.json')

        try:
            with open(self.file_path, 'r') as file:
                self.timestamps = json.load(file)
        except FileNotFoundError:
            self.timestamps = {}

    def get_file_timestamp(self, file_path):
        return os.path.getmtime(file_path)

    def isTimestampNew(self, actualTimestamp, file_name):
        lastTimestamp = self.timestamps.get(file_name)

        if isinstance(lastTimestamp, float):
            if math.isclose(lastTimestamp, actualTimestamp, rel_tol=1e-09, abs_tol=0.0):
                return False

        return True

    def saveTimestamps(self, actualTimestamp, file_name):
        self.timestamps.update({file_name: actualTimestamp})

        with open(self.file_path, 'w') as file:
            json.dump(self.timestamps, file, indent=4)


@dataclass
class FileUpdater:
    file_name: str = None
    file_path_csv: str = None
    actualTimestamp: float = None
    timestampAnalyzer: TimestampAnalizer = field(init=False, default_factory=TimestampAnalizer)

    def __post_init__(self):
        self.map = {
            'encoding': {'SOURCE_TO_CONCEPT_MAP': 'utf-8-sig', 'LOCAL_TO_LOCAL_MAP': 'ISO-8859-1'},
            'sep': {'CONCEPT': '\t', 'CONCEPT_RELATIONSHIP': '\t'},
            'columns': {
                'CONCEPT': ['concept_id', 'concept_code'], 'CONCEPT_RELATIONSHIP': ["concept_id_1", "concept_id_2"],
                'SOURCE_TO_CONCEPT_MAP': ['source_concept_id', 'source_vocabulary_id', 'target_concept_id', 'target_vocabulary_id']
                }
        }

    def updateFileIfNecessary(self, file_path):
        self.file_name = os.path.splitext(os.path.split(file_path)[1])[0]
        if self.__isFileUpdated(file_path):
            self.__updateFile(file_path)

    def __isFileUpdated(self, file_path) -> bool:
        self.__setCSVPathCounterpart(file_path)
        self.actualTimestamp = self.timestampAnalyzer.get_file_timestamp(self.file_path_csv)

        return self.timestampAnalyzer.isTimestampNew(self.actualTimestamp, self.file_name)

    def __updateFile(self, file_path):
        encoding = self.map.get('encoding').get(self.file_name, 'UTF-8')
        sep = self.map.get('sep').get(self.file_name, ';')
        dtype = self.__setColumnTypes()
        df = pd.read_csv(self.file_path_csv, encoding=encoding, sep=sep, dtype=dtype)
        df = self.__adjustColumnTypes(df)
        df.to_parquet(file_path, index=False)

        self.timestampAnalyzer.saveTimestamps(self.actualTimestamp, self.file_name)

    def __setColumnTypes(self):
        dtype = {}
        for column in self.map['columns'].get(self.file_name, []):
            dtype[column] = str
        return dtype

    def __adjustColumnTypes(self, df):
        for column in self.map['columns'].get(self.file_name, []):
            df[column] = df[column].apply(lambda x: self.__remove_decimal(x))
        return df

    def __remove_decimal(self, value):
        value = str(value)
        if value.endswith('.0'):
            value = value[:-2]
        return value

    def __setCSVPathCounterpart(self, file_path):
        base, ext = os.path.splitext(file_path)
        self.file_path_csv = f"{base}.csv"
