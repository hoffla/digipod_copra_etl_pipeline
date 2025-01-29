from dataclasses import dataclass

from models.Processing.Pipelines.BasePipeline import BasePipeline


@dataclass
class DrugExposurePipeline(BasePipeline):
    def process(self):
        pass
