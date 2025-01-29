from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import pandas as pd


@dataclass
class BaseDeserializedObject(ABC):
    @abstractmethod
    def to_dict(self) -> dict:
        pass

    @property
    def isValidInformation(self) -> bool:
        visitData = self.to_dict()
        if visitData:
            for value in visitData.values():
                if isinstance(value, bool) or value:
                    return True
        return False

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    def to_df(self) -> pd.DataFrame:
        data = self.to_dict()
        return pd.DataFrame(data)

    def get_element(self, element):
        return self.__dict__.get(element)
