import json
import os, sys
import select
from datetime import datetime
from pathlib import Path
import traceback

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dataclasses import dataclass, field
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from dotenv import load_dotenv

sys.path.append(os.path.abspath('/data01/digipodFlaskServer/ETLCopraChariteDigiPOD'))
os.environ['HTTP_PROXY'] = "http://proxy.charite.de:8080"
os.environ['HTTPS_PROXY'] = "http://proxy.charite.de:8080"

from models.Utils.logger import get_logger

load_dotenv()

logger = get_logger('sql_table_notification_logger')


@dataclass
class NotifyTriggerManager:
    sqlEngine: Engine = field(init=False)
    notify_channel: str = field(init=False, default="table_change")
    notification_file: Path = field(init=False)

    def __post_init__(self):
        self._initialize_engine()

    @classmethod
    def _initialize_engine(cls):
        host, database, user, password, basePath, projectPath, ressourcePath = [
            cls.__get_env_vars(var) for var in ('HOST', 'DATABASE', 'DBUSER', 'PASSWORD', 'BASEPATH', 'PROJECTDIRECTORY', 'RESSOURCES_DIR')
        ]

        connection_string = f'postgresql+psycopg2://{user}:{password}@{host}/{database}'

        cls.sqlEngine = create_engine(connection_string)
        cls.notification_file = Path(basePath, projectPath, ressourcePath, 'notification_status.json')

    @staticmethod
    def __get_env_vars(var) -> str:
        env_var = os.getenv(var)
        if not env_var:
            logger.critical(f"The environmental variable {var} could not be retrieved!")
            raise RuntimeError
        return env_var

    def _execute_sql(self, sql: str) -> None:
        """Executa um comando SQL bruto e adiciona logging."""
        try:
            with self.sqlEngine.connect() as connection:
                connection.execute(text(sql))
                connection.execute(text('COMMIT;'))
                logger.info(f"Executed SQL: {sql}")
        except Exception as e:
            logger.error(f"Failed to execute SQL: {sql}. Error: {e}")

    def _update_notification_file(self, payload: str) -> None:
        data = {
            "notification_processed": False,
            "last_notification": str(datetime.now()),
            "payload": payload
        }

        try:
            with open(self.notification_file, 'w') as file:
                json.dump(data, file, indent=4)
            logger.info(f"Notification data written to {self.notification_file}")
        except IOError as e:
            logger.error(f"Failed to write notification data to file: {e}")

    def create_trigger(self, schema_name: str, table_name: str) -> None:
        """
        Cria uma função PL/pgSQL e um trigger na tabela especificada para enviar notificações em mudanças.
        :param schema_name: Nome do schema onde a tabela está localizada.
        :param table_name: Nome da tabela onde o trigger será criado.
        """
        # Incluir o schema no nome completo da tabela
        full_table_name = f"{schema_name}.{table_name}"

        create_function_sql = f"""
        CREATE OR REPLACE FUNCTION public.notify_table_change() RETURNS trigger AS $$
        BEGIN
        PERFORM pg_notify('{self.notify_channel}', TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME || ' has changed');
        RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        create_trigger_sql = f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 
                FROM pg_trigger 
                WHERE tgname = '{table_name}_change_trigger' 
                AND tgrelid = '{schema_name}.{table_name}'::regclass
            ) THEN
                CREATE TRIGGER {table_name}_change_trigger
                AFTER INSERT OR UPDATE OR DELETE ON {schema_name}.{table_name}
                FOR EACH ROW EXECUTE FUNCTION public.notify_table_change();
            END IF;
        END;
        $$;
        """
        self._execute_sql(create_function_sql)
        logger.info(f"Trigger function 'notify_table_change' created successfully.")

        self._execute_sql(create_trigger_sql)
        logger.info(f"Trigger 'table_change_trigger' created for table '{full_table_name}'.")


    def listen_for_notifications(self) -> None:
        """
        Escuta notificações enviadas através do canal PostgreSQL NOTIFY. Usa psycopg2 para escutar o canal.
        """
        conn = self.sqlEngine.raw_connection()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        with conn.cursor() as cursor:
            cursor.execute(f"LISTEN {self.notify_channel};")
            logger.info(f"Listening on channel '{self.notify_channel}'...")

            while True:
                if select.select([conn], [], [], 5) == ([], [], []):
                    logger.debug("No notifications received.")
                else:
                    conn.poll()
                    while conn.notifies:
                        notify = conn.notifies.pop(0)
                        logger.info(f"Received notification: {notify.payload}")
                        self._update_notification_file(notify.payload)

    def drop_trigger(self, table_name: str) -> None:
        """
        Remove o trigger de uma tabela específica.
        :param table_name: Nome da tabela onde o trigger será removido.
        """
        drop_trigger_sql = f"DROP TRIGGER IF EXISTS {table_name}_change_trigger ON {table_name};"
        self._execute_sql(drop_trigger_sql)
        logger.info(f"Trigger 'table_change_trigger' removed from table '{table_name}'.")


if __name__ == '__main__':
    while True:
        try:
            manager = NotifyTriggerManager()
            # Criar um trigger para a tabela 'casenumber_mappings'
            manager.create_trigger('cds_cdm', 'casenumber_mappings')
            # Escutar notificações
            manager.listen_for_notifications()
        except KeyboardInterrupt:
            logger.info("Manuelly stopped listening for notifications.")
            raise KeyboardInterrupt
        except Exception as err:
            tracebackString = "".join(traceback.format_exception(type(err), err, err.__traceback__))
            logger.critical(f'A critical error has occurred! Error: {tracebackString}')

    # Para remover o trigger:
    #manager.drop_trigger('casenumber_mappings')
