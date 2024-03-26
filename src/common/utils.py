import logging.config

import os
import time
from datetime import datetime, timezone


def set_timestamp_to_now() -> str:
    return datetime.now(timezone.utc).isoformat()

# Create a Formatter class that logs timestamps as UTC
# time.gmtime is a function that converts a time expressed in seconds since the epoch to a struct_time in UTC
class UTCFormatter(logging.Formatter):
    converter = time.gmtime


LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'utc': {
            '()': UTCFormatter,  # format the timestamp as UTC.
            'format': '[%(asctime)s][%(levelname)s][%(module)s]: %(message)s',
        },
    },
    'handlers': {
        'console': {
            'level': os.getenv("LOG_LEVEL") or "WARNING",
            'class': 'logging.StreamHandler',
            'formatter': 'utc',
        },
    },
    'root': {
        'level': os.getenv("LOG_LEVEL") or "WARNING",
        'handlers': [
            'console'
        ]
    },
}
logging.config.dictConfig(LOGGING_CONFIG)

TransformerLogger = logging.getLogger
