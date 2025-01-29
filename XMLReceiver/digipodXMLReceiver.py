import uuid
import os
import logging
from flask import Flask, request

import sys
import os
sys.path.append(os.path.abspath('/data01/digipodFlaskServer'))

app = Flask(__name__)

from ETLCopraChariteDigiPOD.models.Utils.logger import get_logger

logger = get_logger('xml_receiver_logger')


STORAGE_DIR = './XMLStorage/xml_storage'
STORAGE_DIR_TEST  = './XMLStorage/test_xml_storage'
os.makedirs(STORAGE_DIR, exist_ok=True)
os.makedirs(STORAGE_DIR_TEST, exist_ok=True)


@app.route('/receive-xml', methods=['POST', 'PUT'])
def receive_xml():
    try:
        xml_data = request.get_data(as_text=False)
        if not xml_data.strip():
            logging.warning("Empty request body.")
            return "Request body is empty.", 400

        unique_filename = f"{uuid.uuid4()}.xml"
        xml_filename = os.path.join(STORAGE_DIR, unique_filename)

        try:
            #with open(xml_filename, 'wb') as xml_file:
            #    xml_file.write(xml_data)
            logging.info(f"XML saved successfully as {xml_filename}")
            return "XMLFile received successfully", 200
        except OSError as e:
            logging.error(f"Failed to save XML: {str(e)}")
            return f"Failed to save the XML on the server: {str(e)}", 500

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        return f"Unexpected error: {str(e)}", 500
    


@app.route('/receive-xml-test', methods=['POST', 'PUT'])
def receive_xml_test():
    try:
        xml_data = request.get_data(as_text=False)
        if not xml_data.strip():
            logging.warning("Empty request body.")
            return "Request body is empty.", 400

        unique_filename = f"{uuid.uuid4()}.xml"
        xml_filename = os.path.join(STORAGE_DIR_TEST, unique_filename)

        try:
            with open(xml_filename, 'wb') as xml_file:
                xml_file.write(xml_data)
            logging.info(f"XML saved successfully as {xml_filename}")
            return "XMLFile received successfully", 200
        except OSError as e:
            logging.error(f"Failed to save XML: {str(e)}")
            return f"Failed to save the XML on the server: {str(e)}", 500

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        return f"Unexpected error: {str(e)}", 500


if __name__ == '__main__':
    logging.info("Starting the XML receiving server.")
    app.run(debug=True)
