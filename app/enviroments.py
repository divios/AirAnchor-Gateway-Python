import os

RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'localhost')
PRIV_KEY_PATH = os.getenv('PRIVATE_KEY_FILE', None)
SAWTOOTH_VALIDATOR_URL = os.getenv('SAWTOOTH_VALIDATOR_URL', 'tcp://localhost:4004')
CA_API_URL = os.getenv('CA_API_URL', 'localhost:8761')
MONGO_URL = os.getenv('MONGO_DATABASE_URL', 'localhost:27017')
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'AirAnchor')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'locations')