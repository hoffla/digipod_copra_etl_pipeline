from dataclasses import dataclass

from models.Processing.Pipelines.BasePipeline import BasePipeline


@dataclass
class DeathPipeline(BasePipeline):
    def process(self):
        pass
