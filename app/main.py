
from fastapi import FastAPI
from app.core import Server
from app.model import TransactionRequest, CertificateSignedRequest

import os

PRIV_KEY_PATH = os.getenv('PRIVATE_KEY_FILE', None)
SAWTOOTH_REST_API_URL = os.getenv('SAWTOOTH_REST_API_URL', 'http://localhost:8008')
CA_API_URL = os.getenv('CA_API_URL', 'localhost:8761')

app = FastAPI()
server = Server(priv_key_path=PRIV_KEY_PATH, 
                sawtooth_rest_url=SAWTOOTH_REST_API_URL, ca_url=CA_API_URL)

@app.post("/api/v1/transaction")
async def process_transaction(tr: TransactionRequest):
    server.create_and_send_batch(tr)

@app.get("/test")
async def test():
    server.create_and_send_batch(
        TransactionRequest(
            CertificateSignedRequest(distinguished_name='dron', 
                                    public_key='033c37f3235a45aa48a0bffb51ddb3a7ec1b13ed01659f09834c14aff25132a63c'),
            'mydata'
        ))