import logging, os
import logging.config

from dotenv import load_dotenv

load_dotenv()

logsPath = os.path.join(os.getenv('BASEPATH'), os.getenv('PROJECTDIRECTORY'), os.getenv('LOG_DIR'))

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        },
        'simple': {
            'format': '%(asctime)s - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
        'rotating_file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': os.path.join(logsPath, 'ETL_Pipeline.log'),
                'maxBytes': 10485760,  # 10MB
                'backupCount': 5,
                'formatter': 'standard',
        },
        'rotating_file_xml': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(logsPath, 'XML_Receiver.log'),
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5,
            'formatter': 'standard',
        },
        'rotating_file_notification': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(logsPath, 'Notification.log'),
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5,
            'formatter': 'simple',
        },
        'rotating_file_quarantine': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(logsPath, 'Quarantine.log'),
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5,
            'formatter': 'standard',
        },
    },

    'loggers': {
        '': {
            'handlers': ['console', 'rotating_file'],
            'level': 'DEBUG',
            'propagate': True,
        },
        'xml_receiver_logger': {
            'handlers': ['console', 'rotating_file_xml'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'sql_table_notification_logger': {
            'handlers': ['console', 'rotating_file_notification'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'quarantine_logger': {
            'handlers': ['console', 'rotating_file_quarantine'],
            'level': 'DEBUG',
            'propagate': False,
        }
    }
})


def get_logger(name: str):
    return logging.getLogger(name)