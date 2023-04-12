
from fastapi import FastAPI
from app.core import Server
from app.model import TransactionRequest, CertificateSignedRequest
from app.data import MongoRepo

import os

PRIV_KEY_PATH = os.getenv('PRIVATE_KEY_FILE', None)
SAWTOOTH_VALIDATOR_URL = os.getenv('SAWTOOTH_VALIDATOR_URL', 'tcp://localhost:4004')
CA_API_URL = os.getenv('CA_API_URL', 'localhost:8761')
MONGO_URL = os.getenv('MONGO_DATABASE_URL', 'localhost:27017')
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'AirAnchor')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'locations')


app = FastAPI()

mongoRepo = MongoRepo(mongo_url=MONGO_URL, 
                      mongo_database=MONGO_DATABASE, mongo_collection=MONGO_COLLECTION)

server = Server(priv_key_path=PRIV_KEY_PATH, sawtooth_validator_url=SAWTOOTH_VALIDATOR_URL, 
                mongo_repo=mongoRepo, ca_url=CA_API_URL)

@app.post("/api/v1/transaction")
async def process_transaction(tr: TransactionRequest):
    server.create_and_send_batch(tr)
