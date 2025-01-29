from dataclasses import dataclass

from models.Processing.Pipelines.BasePipeline import BasePipeline


@dataclass
class DeviceExposurePipeline(BasePipeline):
    def process(self):
        pass
