import traceback
from dataclasses import dataclass, field

from models.Processing.Pipelines.PersonPipeline import PersonPipeline
from models.Processing.Pipelines.VisitOccurrencePipeline import VisitOccurrencePipeline
from models.Processing.Pipelines.VisitDetailPipeline import VisitDetailPipeline
from models.Processing.Pipelines.DeathPipeline import DeathPipeline
from models.Processing.Pipelines.ProcedureOccurrencePipeline import ProcedureOccurrencePipeline
from models.Processing.Pipelines.MeasurementPipeline import MeasurementPipeline
from models.Processing.Pipelines.ObservationPipeline import ObservationPipeline
from models.Processing.Pipelines.ConditionOccurrencePipeline import ConditionOccurrencePipeline
from models.Processing.Pipelines.DrugExposurePipeline import DrugExposurePipeline
from models.Processing.Pipelines.DeviceExposurePipeline import DeviceExposurePipeline

from models.PipelineStarter.event import post_event
from models.Utils.logger import get_logger

logger = get_logger(__name__)

pipelines = {
    'person': PersonPipeline,
    'visit_occurrence': VisitOccurrencePipeline,
    'visit_detail': VisitDetailPipeline,
    'death': DeathPipeline,
    'procedure_occurrence': ProcedureOccurrencePipeline,
    'measurement': MeasurementPipeline,
    'observation': ObservationPipeline,
    'condition_occurrence': ConditionOccurrencePipeline,
    'drug_exposure': DrugExposurePipeline,
    'device_exposure': DeviceExposurePipeline,
}


@dataclass
class PipelineManager:
    pipelines: dict = field(init=False)

    def __post_init__(self):
        self.pipelines = pipelines

    def processData(self, dependencies, tableName):
        omopColumns = post_event('getTableColumns', tableName)
        pipeline = self.pipelines.get(tableName)(dependencies, omopColumns, tableName)
        processedData = pipeline.process()
        post_event('saveDataframe', tableName, processedData) # Todo: Only for debugging
        processedDataTypeEnforced = pipeline.enforceDataType(processedData)
        return processedDataTypeEnforced
