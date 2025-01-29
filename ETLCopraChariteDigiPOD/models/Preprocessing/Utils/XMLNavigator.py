import chardet
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Union, Optional
from dataclasses import dataclass

from models.Utils.logger import get_logger


logger = get_logger(__name__)


@dataclass
class XMLLoader:
    def load_content(self, source: Union[str, bytes, Path]) -> str:
        if isinstance(source, Path) or (isinstance(source, str) and Path(source).is_file()):
            return self._load_from_file(source)
        elif isinstance(source, bytes):
            return self._decode_bytes(source)
        return source  # Assumes `source` is already a decoded string

    def _load_from_file(self, file_path: Union[str, Path]) -> str:
        with open(file_path, 'rb') as file:
            content = file.read()
            return self._decode_bytes(content)

    def _decode_bytes(self, content: bytes) -> str:
        try:
            detection = chardet.detect(content)
            detected_encoding = detection['encoding']
            confidence = detection['confidence']
            
            print(f"Detected encoding: {detected_encoding} with confidence {confidence}")

            # If confidence is high, decode using detected encoding
            if confidence > 0.8:
                return content.decode(detected_encoding)

            # Check BOM for UTF-16 manually if chardet confidence is low
            if content.startswith(b'\xff\xfe') or content.startswith(b'\xfe\xff'):
                logger.debug("BOM identified for UTF-16. Decoding xml file using UTF-16")
                return content.decode('utf-16')
            elif b'encoding="utf-16"' in content[:100]:
                logger.debug("BOM not identified for UTF-16. Trying to decode xml file using UTF-16")
                return content.decode('utf-16-le' if content[0] == 0xff else 'utf-16-be')
            else:
                logger.debug("No encoding declared; falling back to UTF-8")
                return content.decode('utf-8')  # Default to UTF-8 if no specific encoding is declared
            
        except UnicodeDecodeError as e:
            logger.error(f"Decoding error occurred. Error: {e}. Falling back to UTF-8")
            return content.decode('utf-8', errors='ignore')

@dataclass
class XMLNavigator:
    xml_source: Union[str, bytes]
    loader: XMLLoader = XMLLoader()

    def __post_init__(self):
        content = self.loader.load_content(self.xml_source)

        logger.debug("XML Content (beginning of file):  %s", content[:100])
        
        if not content.strip():
            logger.error(f"XML file ({self.xml_source}) content is empty or is invalid")
            raise ValueError(f"XML file ({self.xml_source}) content is empty or is invalid")
        
        self.tree = ET.ElementTree(ET.fromstring(content))
        self.root = self.tree.getroot()

    def find_elements(self, path: str) -> list:
        return self.root.findall(path)

    def find_element(self, path: str) -> Optional[ET.Element]:
        return self.root.find(path)

    @staticmethod
    def get_element_value(element, attribute_name=None, element_nullable=False):
        if element_nullable:
            if element is None:
                return None

        if attribute_name:
            return element.attrib.get(attribute_name, None)
        else:
            return element.text
