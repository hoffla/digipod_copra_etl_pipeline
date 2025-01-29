from models.PipelineStarter.InitializeToolsBuilder.Builders import StandardOMOPMapperBuilder
from models.PipelineStarter.event import subscribe
from models.Utils.DataframeErrorManager import DataframeErrorManager
from models.Utils.NotificationStatusChecker import NotificationStatusChecker
from models.Utils.PipelineCodeStatusMonitor import PipelineCodeStatusMonitor
from models.Utils.State import ExceptionsRaisedDetector, TableUpdateStatusDetector
from models.Utils.PatientFilter import PatientFilter
from models.Utils.SQLInteractor import exportTable, importTable, SQLDataUpdater
from models.Utils.SaveDataframe import DataFrameSaver
from models.Utils.SendEmail import send_email, send_email_smtp, send_mail_intern
from models.PipelineStarter.Ressources.OMOPSchemas import OMOPTablesAttributesHandler
from models.Utils.UniqueIDCreator import UniqueIDCreator


def handle_send_email(subject, message, recipients='RECIPIENTS_EMAIL', onlyIntern=False, onlyExtern=False):
    if onlyIntern:
        send_mail_intern(subject, message, recipients)
        #send_email_smtp(subject, message, recipients)
    elif onlyExtern:
        send_email(subject, message, recipients)
    else:
        send_mail_intern(subject, message, recipients)
        #send_email_smtp(subject, message, recipients)
        send_email(subject, message, recipients)


def setup_sqldatabase_interactor_handlers():
    sqlDataUpdater = SQLDataUpdater()
    patFilter = PatientFilter()

    subscribe('importTable', importTable)
    subscribe('exportTable', exportTable)
    subscribe('updateTable', sqlDataUpdater.updateTable)
    subscribe('filterXMLFiles', patFilter.filterXMLFiles)
    subscribe('getPersonID', patFilter.getPersonID)


def setup_omop_att_handlers():
    subscribe('getTableDependecies', OMOPTablesAttributesHandler.getTableDependecies)
    subscribe('getTableSchema', OMOPTablesAttributesHandler.getTableSchema)
    subscribe('getTableColumns', OMOPTablesAttributesHandler.getTableColumns)
    subscribe('getOMOPTableNames', OMOPTablesAttributesHandler.getOMOPTableNames)


def setup_utils_handlers():
    dfSaver = DataFrameSaver()
    exceptionRaisedDetector = ExceptionsRaisedDetector()
    dfErrorManager = DataframeErrorManager()
    pipelineCodeMonitor = PipelineCodeStatusMonitor()
    notificationStatusChecker = NotificationStatusChecker()
    updateStatusDetector = TableUpdateStatusDetector()

    subscribe('sendEmail', handle_send_email)
    subscribe('saveDataframe', dfSaver.save)
    subscribe('createUniqueID', UniqueIDCreator.createUniqueID)

    subscribe('setExceptionStatus', exceptionRaisedDetector.set_exception_status)
    subscribe('getExceptionStatus', exceptionRaisedDetector.get_exception_status)
    subscribe('resetAllExceptions', exceptionRaisedDetector.reset_all_status)

    subscribe('saveErrorDataframe', dfErrorManager.save)
    subscribe('loadErrorDataframe', dfErrorManager.load)
    subscribe('listErrorDataframe', dfErrorManager.list_tables)
    subscribe('deleteErrorDataframe', dfErrorManager.delete)
    subscribe('clearAllErrorDataframe', dfErrorManager.clear_all)

    subscribe('checkChangesInScript', pipelineCodeMonitor.check_changes_file_for_table)

    subscribe('checkChangesInCaseMappings', notificationStatusChecker.check_and_update_status)

    subscribe('setTableUpdateStatus', updateStatusDetector.set_update_status)
    subscribe('informIfUpdates', updateStatusDetector.inform_if_updates)


def setup_conceptID_domainID_handlers():
    omopMapperBuilder = StandardOMOPMapperBuilder()
    omopMapper = omopMapperBuilder.getOMOPmapper()

    subscribe('addConceptIDCols', omopMapper.addConceptIDCols)
    subscribe('mapLocalCodeToLocal', omopMapper.mapLocalCodeToLocal)
    subscribe('mapSourceConceptToConcepts', omopMapper.mapSourceConceptToConcepts)
    subscribe('mapSourceCodeToConcepts', omopMapper.mapSourceCodeToConcepts)
    subscribe('mapConceptsToStandardConcepts', omopMapper.mapConceptsToStandardConcepts)
