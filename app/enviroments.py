import os
import logging


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger('base')
logging.basicConfig()


PRIV_KEY_PATH = os.getenv('PRIVATE_KEY_FILE', None)

RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'localhost')
SAWTOOTH_VALIDATOR_URL = os.getenv('SAWTOOTH_VALIDATOR_URL', 'tcp://localhost:4004')
CA_API_URL = os.getenv('CA_API_URL', 'localhost:8761')

MONGO_URL = os.getenv('MONGO_DATABASE_URL', 'localhost:27017')
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'AirAnchor')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'locations')

BUFFER_MAX_SIZE = os.getenv('BUFFER_MAX_SIZE', 80)

TOKEN_RATE = int(os.getenv('TOKEN_BUCKET_RATE', 5))
TOKEN_CAPACITY = int(os.getenv('TOKEN_BUCKET_CAPACITY', 30))

LEAKY_BUCKET_LIMIT = int(os.getenv('LEAKY_BUCKET_LIMIT', 10))
