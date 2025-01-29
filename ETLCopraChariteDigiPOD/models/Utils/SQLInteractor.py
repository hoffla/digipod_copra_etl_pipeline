import copy
import os
import re
import traceback
import pandas as pd

from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy.exc import DatabaseError, SQLAlchemyError
from sqlalchemy import create_engine, text, Table, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert

from models.PipelineStarter.Ressources.OMOPSchemas import OMOPTablesAttributesHandler
from models.PipelineStarter.event import post_event
from models.Utils.logger import get_logger

logger = get_logger(__name__)


class SQLEngineSingleton:
    _instance = None
    _engine = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SQLEngineSingleton, cls).__new__(cls)
            cls._initialize_engine()
        return cls._instance

    @classmethod
    def _initialize_engine(cls):
        host, database, user, password = [
            cls.__get_env_vars(var) for var in ('HOST', 'DATABASE', 'DBUSER', 'PASSWORD')
        ]

        connection_string = f'postgresql+psycopg2://{user}:{password}@{host}/{database}'

        cls._engine = create_engine(connection_string)

    @classmethod
    def __get_env_vars(cls, var) -> str:
        env_var = os.getenv(var)
        if not env_var:
            logger.critical(f"The environmental variable {var} could not be retrieved!")
            raise RuntimeError
        return env_var

    @classmethod
    def get_or_create_engine(cls):
        if cls._engine is None:
            cls._instance = cls()
        return cls._engine


def getOrCreateSQLEngine():
    return SQLEngineSingleton.get_or_create_engine()


def _find_id_column(df):
    id_column = df.filter(regex='id$').columns[0]
    return id_column


def joinTables(processedTable, oldTable):
    try:
        oldTable_id = _find_id_column(oldTable)
        processedTable_id = _find_id_column(processedTable)

        if oldTable_id == processedTable_id:
            oldTable.set_index(oldTable_id, inplace=True)
            processedTable.set_index(processedTable_id, inplace=True)

            df_combined = processedTable.combine_first(oldTable).reset_index()
            logger.info("Tables were successfully combined.")

            return df_combined

    except AttributeError as err:
        logger.error(
            f'One of the arguments passed to the function was not a Dataframe: \nold Dataframe {type(oldTable)} - new Dataframe {type(processedTable)}. Error: {err}')
    except IndexError as err:
        logger.error(
            f'The ID Column could not be found: \nold Dataframe {oldTable.columns} - new Dataframe {processedTable.columns}. Error: {err}')


def importTable(tableName, engine, schema='cds_cdm', **kwargs) -> pd.DataFrame:
    try:
        tableName = schema.rstrip('.') + '.' + tableName
        query = _generateQuery(tableName, **kwargs)
        df = pd.read_sql(query, engine)
        logger.debug(f"Successfully imported the dataframe '{tableName}' from the SQL Databank!")
        return df
    except TypeError as err:
        logger.error(f'The following table "{tableName}" could not be retrieved. Error: {err}')
    except ValueError as err:
        logger.error(f'Invalid query or issue with reading SQL data for table "{tableName}". Error: {err}')
    except DatabaseError as err:
        logger.error(f'Database connectivity issue when retrieving table "{tableName}". Error: {err}')
    except SQLAlchemyError as err:
        logger.error(f'Database error occurred when retrieving table "{tableName}". Error: {err}')
    except Exception as err:
        logger.error(f'An unexpected error occurred when retrieving table "{tableName}". Error: {err}')


def _generateQuery(tableName, **kwargs) -> str:
    columns = kwargs.get('columns', '*')
    cast_columns = kwargs.get('cast_columns', {})

    if isinstance(columns, list):
        columns_part = ', '.join(
            f"CAST({col} AS {cast_columns[col]}) AS {col}" if col in cast_columns else col
            for col in columns
        )
    else:
        columns_part = columns
    
    query = f'SELECT {columns_part} FROM {tableName}'

    if 'joins' in kwargs:
        query += ' ' + ' '.join(kwargs['joins'])

    if where := kwargs.get('where'):
        query += ' WHERE ' + _build_filter_clause(where)

    optional_clauses = {
        'group_by': " GROUP BY {}",
        'having': " HAVING {}",
        'order_by': " ORDER BY {}",
        'limit': " LIMIT {}",
        'offset': " OFFSET {}",
        'distinct': "DISTINCT {}",
    }

    for key, clause in optional_clauses.items():
        if key in kwargs:
            if key == 'distinct':
                if kwargs[key]: query = query.replace("SELECT", f"SELECT DISTINCT")
            else:
                query += clause.format(kwargs[key])

    return query


def _build_filter_clause(filter_list: list) -> str:
    clauses = []
    for item in filter_list:
        if isinstance(item, tuple):
            column, operator, value = item
            if isinstance(value, str):
                value = 'NULL' if value.upper() == 'NULL' else f"'{value}'"
            elif isinstance(value, tuple):
                value = f"({', '.join(map(str, value))})" if len(value) > 1 else f"({value[0]})"
            clauses.append(f"{column} {operator} {value}")
        elif isinstance(item, list):
            clauses.append(f"({_build_filter_clause(item)})")
        else:
            clauses.append(item)
    return ' '.join(clauses)


def exportTable(df, tableName, engine, schema='cds_cdm') -> None:
    if not df.empty:
        with engine.begin() as conn:
            conn.execute(text(f'DELETE FROM {schema}.{tableName};'))
            logger.debug(f"Successfully deleted all rows of the table '{schema}.{tableName}'! Still not committed.")

            df.to_sql(tableName, con=conn, schema=schema, if_exists="append", index=False)
            logger.debug(
                f"Successfully exported the new dataframe '{schema}.{tableName}' to the SQL Databank! Still not commited.")

            conn.execute(text('COMMIT;'))
        logger.debug(f"Successfully commited all the changes to the dataframe '{schema}.{tableName}'.")

    else:
        logger.error(f'The following table {tableName} was empty and therefore not exported.')


@dataclass
class SQLDataUpdater:
    sqlEngine: Engine = field(init=False)
    attempt: int = field(init=False, default=1)
    tableName: str = field(init=False)
    schema: str = field(init=False)

    def __post_init__(self):
        self.sqlEngine = getOrCreateSQLEngine()

    def updateTable(self, df, tableName, schema='cds_cdm') -> None:
        self.__setMetadata(tableName, schema)
        func = self.__defineUpdateMethod()

        while isinstance(df, pd.DataFrame) and not df.empty:
            try:
                func(df)
                post_event('setTableUpdateStatus', tableName, True)
                break
            except DatabaseError as err:
                post_event('setExceptionStatus', tableName, True)
                self._handle_export_error(df, err)
        else:
            logger.info(f'The following table {tableName} was empty and therefore not exported.')

    def __setMetadata(self, tableName, schema):
        self.tableName = tableName
        self.schema = schema
        self.attempt = 1

    def __defineUpdateMethod(self) -> callable:
        if self.tableName in ('person', 'casenumber_mappings'):
            return self._attempt_upsert
        return self._attempt_export

    def _attempt_upsert(self, df):
        updateCols = copy.deepcopy(OMOPTablesAttributesHandler.getTableColumns(self.tableName))
        conflictCols = [updateCols.pop(0)]

        metadata = MetaData()
        table = Table(self.tableName, metadata, schema=self.schema, autoload_with=self.sqlEngine)

        upsert_query = self._create_upsert_query(table, df, conflictCols, updateCols)
        with self.sqlEngine.begin() as conn:
            conn.execute(upsert_query)
            conn.execute(text('COMMIT;'))

        post_event('setUpdateStatus', self.tableName, True)
        logger.info(f"UPSERT from table {self.tableName} was successfull.")

    @staticmethod
    def _create_upsert_query(table, df, conflictCols, updateCols):
        records = df.to_dict(orient='records')

        insert_stmt = insert(table).values(records)
        on_conflict_stmt = insert_stmt.on_conflict_do_update(
            index_elements=conflictCols,
            set_={col.name: insert_stmt.excluded[col.name] for col in table.columns if col.name in updateCols}
        )

        return on_conflict_stmt

    def _attempt_export(self, df):
        oldTable = importTable(self.tableName, self.sqlEngine) # Veio empty aqui!!
        joinedTable = joinTables(df, oldTable) # Veio empty aqui!!
        exportTable(joinedTable, self.tableName, self.sqlEngine, self.schema)

    def _handle_export_error(self, df, error):
        traceback_string = self._get_traceback_string(error)
        logger.error(f"Error detected: {traceback_string}")

        faulty_rows_df = self._identify_faulty_rows(df, traceback_string)
        if faulty_rows_df is not None:
            self._save_faulty_rows(faulty_rows_df)
            df.drop(faulty_rows_df.index, inplace=True) # Todo: pode ser que isso nao funcione!!
        else:
            logger.error(f"Faulty rows weren't identified in the traceback string. Saving traceback locally.")
            self.save_string_to_txt(traceback_string, self.tableName, self.attempt)
            RuntimeError(f"Faulty rows of table '{self.tableName}' could not be dynamically identified!")

    @staticmethod
    def _get_traceback_string(e):
        return "".join(traceback.format_exception(type(e), e, e.__traceback__))

    @staticmethod
    def _identify_faulty_rows(df, traceback_string):
        error_match = re.search(
            r'null value in column "(?P<column>.*?)".*?violates not-null constraint|'  # Not-null constraint errors
            r'DETAIL:  Key \((?P<unique_column>.*?)\)=\((?P<value>.*?)\) already exists|'  # Unique constraint errors
            r'insert or update on table.*?violates foreign key constraint.*?DETAIL:  Key \((?P<fk_column>.*?)\)=\((?P<fk_value>.*?)\) is not present',  # Foreign key constraint errors
            traceback_string,
            re.IGNORECASE
        )
        if error_match:
            group_dict = error_match.groupdict()
            column_name = group_dict.get('column') or group_dict.get('unique_column') or group_dict.get('fk_column')
            value = group_dict.get('value') or group_dict.get('fk_value')
            return df[df[column_name].isna()] if value is None else df[
                df[column_name].astype(str).str.strip() == value.strip()]
        return None

    def _save_faulty_rows(self, faulty_df):
        filename = f"faulty_rows__{self.tableName}_attempt_{self.attempt}.csv"
        filePath = os.path.join(os.getenv('BASEPATH'), os.getenv('PROJECTDIRECTORY'), 'FaltyRows', 'Dataframes', filename)
        os.makedirs(os.path.dirname(filePath), exist_ok=True)
        faulty_df.to_csv(filePath, sep=';', index=False)
        logger.info(f"Faulty rows saved to: {filePath}")
        self.attempt += 1

    @staticmethod
    def save_string_to_txt(string, tableName, attempt):
        date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"traceback_{tableName}_attempt_{attempt}_{date_str}.txt"
        filePath = os.path.join(os.getenv('BASEPATH'), os.getenv('PROJECTDIRECTORY'), 'FaltyRows', 'Tracebacks', filename)
        os.makedirs(os.path.dirname(filePath), exist_ok=True)
        with open(filePath, "w") as file:
            file.write(string)
        logger.debug(f"String saved to: {filePath}")
