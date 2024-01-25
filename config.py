import os
from dotenv import find_dotenv, load_dotenv
from logging.config import dictConfig


ROOT_ABS_DIR = os.path.abspath(os.path.dirname(__file__))

load_dotenv(find_dotenv())

BOOTSTRAP_SERVER = os.environ.get('BOOTSTRAP_SERVER')
REDIS_BROKER_URL = os.environ.get('REDIS_BROKER_URL')
REDIS_BACKEND_URL = os.environ.get('REDIS_BACKEND_URL')
MAX_ROW_PER_CSV = os.environ.get('MAX_ROW_PER_CSV')
FLUSH_INTERVAL_SEC = os.environ.get('FLUSH_INTERVAL_SEC')
MAX_BUFFER_LEN = os.environ.get('MAX_BUFFER_LEN')
