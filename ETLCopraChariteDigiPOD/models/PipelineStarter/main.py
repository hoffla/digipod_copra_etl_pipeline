import time, sys, os, traceback

from dotenv import load_dotenv

sys.path.append(os.path.abspath('/data01/digipodFlaskServer/ETLCopraChariteDigiPOD'))
os.environ['HTTP_PROXY'] = "http://proxy.charite.de:8080"
os.environ['HTTPS_PROXY'] = "http://proxy.charite.de:8080"
os.environ['NO_PROXY'] = '.charite.de,localhost'

from models.PipelineStarter.Observer import setup_sqldatabase_interactor_handlers, setup_omop_att_handlers, \
    setup_conceptID_domainID_handlers, setup_utils_handlers
from models.PipelineStarter.InitializeToolsBuilder.Builders import StandardETLPipelineBuilder
from models.PipelineStarter.event import post_event
from models.Utils.logger import get_logger


load_dotenv()

logger = get_logger(__name__)

setup_sqldatabase_interactor_handlers()
setup_omop_att_handlers()
setup_utils_handlers()
setup_conceptID_domainID_handlers()


if __name__ == '__main__':
    runMode = False if os.getenv('RUN_MODE') == 'PROD' else True
    etlPipelineBuilder = StandardETLPipelineBuilder()
    omopETL = etlPipelineBuilder.getPipeline(debug=runMode)

    while True:
        try:
            start = time.time()
            omopETL.startPipeline()
            omopETL.check_quarantine_files()
            post_event('informIfUpdates')
            logger.info(f"ETL-Pipeline finished in {(time.time() - start) / 60:.2f} minutes.")
        except Exception as err:
            tracebackString = "".join(traceback.format_exception(type(err), err, err.__traceback__))
            logger.critical(f"A critical error occurred during the run of ETL-Pipeline. Error: {tracebackString}")
            post_event('sendEmail', "A critical error occurred during the run of ETL-Pipeline", f"Error: {tracebackString}")
        finally:
            time.sleep(30)

    #start = time.time()
    #etlPipelineBuilder = StandardETLPipelineBuilder()
    #omopETL = etlPipelineBuilder.getPipeline(debug=True)
    #omopETL.startPipeline()
    #logger.info(f"ETL-Pipeline finished in {(time.time() - start) / 60:.2f} minutes.")
