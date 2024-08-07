import os
from dotenv import find_dotenv, load_dotenv
from logging.config import dictConfig


ROOT_ABS_DIR = os.path.abspath(os.path.dirname(__file__))

load_dotenv(find_dotenv())

ONESIGNAL_APP_ID = os.environ.get('ONESIGNAL_APP_ID')
ONESIGNAL_REST_API_KEY = os.environ.get('ONESIGNAL_REST_API_KEY')
ONESIGNAL_USER_AUTH_KEY = os.environ.get('ONESIGNAL_USER_AUTH_KEY')
BOOTSTRAP_SERVER = os.environ.get('BOOTSTRAP_SERVER')
REDIS_BROKER_URL = os.environ.get('REDIS_BROKER_URL')
REDIS_BACKEND_URL = os.environ.get('REDIS_BACKEND_URL')
MAX_ROW_PER_CSV = os.environ.get('MAX_ROW_PER_CSV')
FLUSH_INTERVAL_SEC = os.environ.get('FLUSH_INTERVAL_SEC')
MAX_BUFFER_LEN = os.environ.get('MAX_BUFFER_LEN')
USER_LOG_INTERVAL_MINUTES = os.environ.get('USER_LOG_INTERVAL_MINUTES')
LIVE_UPDATE_GET_LOG_FROM_LAST_HOUR = os.environ.get('LIVE_UPDATE_GET_LOG_FROM_LAST_HOUR')
ONESIGNAL_NOTIF_BLAST_ENDPOINT = os.environ.get('ONESIGNAL_NOTIF_BLAST_ENDPOINT')
LIVE_UPDATE_GET_INACTIVE_USER_FROM_HOUR = os.environ.get('LIVE_UPDATE_GET_INACTIVE_USER_FROM_HOUR')