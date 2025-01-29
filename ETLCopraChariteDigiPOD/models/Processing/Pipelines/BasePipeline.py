import datetime
import random
import re
from typing import Callable
import pandas as pd

from pandas import DataFrame
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from sqlalchemy.engine import Engine

from models.PipelineStarter.Ressources.Timezone import timezone
from models.PipelineStarter.event import post_event
from models.Utils.SQLInteractor import getOrCreateSQLEngine
from models.Utils.logger import get_logger


logger = get_logger(__name__)

# TODO: Adicionar capacidade de poder ler tables do SQL! Ou seja, só pegar o sqlEngine e mandar um post_event com o tableName


@dataclass
class BasePipeline(ABC):
    rawData: dict = field(repr=False)
    omopColumns: list = field(repr=False)
    tableCDM: str = field(repr=False)
    idCol: str = field(init=False, repr=False)

    sqlEngine: Engine = field(init=False, repr=False, default_factory=getOrCreateSQLEngine)

    def __post_init__(self):
        self.idCol = self.tableCDM + '_id'

    @abstractmethod
    def process(self) -> DataFrame:
        pass

    def enforceDataType(self, df):
        if isinstance(df, pd.DataFrame):
            tablesSchema = post_event('getTableSchema', self.tableCDM)
            for col, dtype in tablesSchema.items():
                try:
                    df = self.__tryEnforceDataTypeOnCol(df, col, dtype)
                except (TypeError, ValueError) as err:
                    logger.error(
                        f"Column '{col}' of table '{self.tableCDM}' could not be cast into type '{dtype}'. Error: {err}")

            return df
    
    def __tryEnforceDataTypeOnCol(self, df, col, dtype):
        if df[col].dtype != dtype:
            if isinstance(dtype, str) and dtype.startswith("datetime"):
                df = self.__handleDatetimeTypes(df, col, dtype)
            else:
                df[col] = df[col].astype(dtype)
                    
        return df

    @staticmethod
    def __handleDatetimeTypes(df, col, dtype):
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(dtype)
            df[col] = df[col].dt.tz_localize(timezone)
            logger.info(f"Column '{col}' converted to datetime followed by timezone-aware convertion with Berlin timezone.")
        elif not pd.api.types.is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(timezone)
            logger.info(f"Column '{col}' converted to timezone-aware with Berlin timezone.")
        return df

    @staticmethod
    def _createUniqueID(*args, **kwargs):
        return post_event('createUniqueID', *args, **kwargs)

    @staticmethod
    def _addPersonID(*args, **kwargs):
        return post_event('getPersonID', *args, **kwargs)

    @staticmethod
    def _handleDuplicatedRows(df_selected):
        duplicates = df_selected.duplicated(subset='combined', keep=False)

        df_selected.loc[duplicates, 'combined'] = df_selected['combined'] + \
            df_selected.loc[duplicates].groupby('combined').cumcount().astype(str).radd('_')

        return df_selected

    @staticmethod
    def _addColPerConvertion(df, oldCol: str, newCol: str, colType: Callable):
        conversion_funcs = {
            int: lambda x: pd.to_numeric(x, errors='coerce').astype("Int64"),
            float: lambda x: pd.to_numeric(x, errors='coerce'),
            pd.Timestamp: lambda x: pd.to_datetime(x, errors='coerce'),
            datetime.datetime: lambda x: pd.to_datetime(x, errors='coerce')
        }

        def safe_conversion(val):
            try:
                return colType(val)
            except (ValueError, TypeError):
                return None
            
        convert_func = conversion_funcs.get(colType, lambda x: x.apply(safe_conversion))
        
        df[newCol] = convert_func(df[oldCol])
        return df

    def _adaptSchema(self, *dfs) -> DataFrame:
        adaptedDfs = []
        for df in dfs:
            if not df.empty:
                for col in self.omopColumns:
                    if not col in df.columns:
                        df[col] = None
                adaptedDfs.append(df)
        concatenedDf = pd.concat(adaptedDfs, ignore_index=True)
        return concatenedDf[self.omopColumns].drop_duplicates()

    @staticmethod
    def _renameCols(*dfs, mapping=None) -> None:
        for df in dfs:
            df.rename(columns=mapping, inplace=True)

    def _colsToRemove(self, df, columns_to_remove, keepCopy):
        allColsToRemove = self.__identifyMergedCols(df, columns_to_remove)
        df, allColsToRemove = self.__keepOneCopy(df, allColsToRemove, keepCopy)
        df = df.loc[:, ~df.columns.isin(allColsToRemove)]
        return df

    @staticmethod
    def __identifyMergedCols(df, cols_to_remove):
        allColsToRemove = set()

        for colToRemove in cols_to_remove:
            for col in df.columns:
                match = re.match(rf'^({colToRemove})(_.*)?$', col)
                if match:
                    allColsToRemove.add(match.group(0))

        return allColsToRemove

    @staticmethod
    def __keepOneCopy(df, allColsToRemove, keepCopy):
        lengthBegin = len(allColsToRemove)

        for toCopyCol in keepCopy:
            if toCopyCol in allColsToRemove:
                allColsToRemove.remove(toCopyCol)
            else:
                for col in allColsToRemove:
                    if col.startswith(toCopyCol) > 0:
                        df.rename(columns={col: toCopyCol}, inplace=True)
                        allColsToRemove.remove(col)
                        break

            if lengthBegin == len(allColsToRemove):
                logger.warning(f'No matching column to "{toCopyCol}" could be found in "{allColsToRemove}"')

        return df, allColsToRemove

    def _addOMOPConceptCols(self, df):
        dfMeltedWithLocal = self.__meltAndAddLocalCodes(df)
        dfWithConcepts = self.__addConceptsToSourceCodes(dfMeltedWithLocal)
        dfWithStandardConcepts = self.__addStandardConceptsToConcepts(dfWithConcepts)
        dfWithConceptIDColumn = self.__addConceptIDCols(dfWithStandardConcepts)

        df = df.merge(dfWithConceptIDColumn, how='left', on=self.idCol)
        return df

    def __meltAndAddLocalCodes(self, df):
        df_melted = df.melt(id_vars=[self.idCol], var_name='variabel', value_name='source_code')
        df_meltedWithLocal = post_event('mapLocalCodeToLocal', df_melted, 'variabel')
        return df_meltedWithLocal

    def __addConceptsToSourceCodes(self, df):
        df = self.__addNewRows(df)
        dfWithConcepts = post_event('mapSourceCodeToConcepts', df, self.idCol)
        return dfWithConcepts

    @staticmethod
    def __addNewRows(df) -> pd.DataFrame:
        mask = df['mapsTo'] == 'Name'
        newRows = df[mask].copy()
        newRows.loc[mask, 'source_code'] = newRows.loc[mask, 'variabel']
        dfWithNewRows = pd.concat([df, newRows], ignore_index=True)
        return dfWithNewRows

    def __addStandardConceptsToConcepts(self, df):
        df['concept_id_1'] = df['concept_id_1'].astype('str')
        dfWithStandardConcepts = post_event('mapConceptsToStandardConcepts', df, self.idCol)
        return dfWithStandardConcepts

    def __addConceptIDCols(self, df):
        dfWithConceptIDColumn = post_event('addConceptIDCols', df, self.tableCDM, self.idCol)
        return dfWithConceptIDColumn

    def _addNonStandardOMOPConceptCols(self, df, conceptCol):
        dfWithConcepts = post_event('mapSourceConceptToConcepts', df, self.idCol, conceptCol)
        df = df.merge(dfWithConcepts, how='left', on=self.idCol)
        return df

    def _importOMOPTable(self, tableName, **kwargs):
        return post_event('importTable', tableName, self.sqlEngine, **kwargs)
