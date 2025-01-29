from abc import ABC, abstractmethod

from dataclasses import dataclass
from typing import List, Optional

from models.Preprocessing.Utils.XMLNavigator import XMLNavigator


@dataclass
class BaseDeserializer(ABC):
    navigator: XMLNavigator

    @abstractmethod
    def deserialize(self) -> List:
        pass

    def _get_element_value(self, path: str, attribute_name: str = "VALUE", nullable=True) -> Optional[str]:
        element = self.navigator.find_element(path)
        return self.navigator.get_element_value(element, attribute_name, element_nullable=nullable)