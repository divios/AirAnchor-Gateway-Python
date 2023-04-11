

import requests
import hashlib
import random
import os
import cbor
import secrets

from binascii import hexlify
from time import time

from app.model import TransactionRequest, CertificateSignedRequest

from sawtooth_signing.secp256k1 import Secp256k1PublicKey
from sawtooth_signing import create_context
from sawtooth_signing import CryptoFactory
from sawtooth_signing import ParseError
from sawtooth_signing.secp256k1 import Secp256k1PrivateKey

from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader
from sawtooth_sdk.protobuf.transaction_pb2 import Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchList
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader
from sawtooth_sdk.protobuf.batch_pb2 import Batch

from fastapi import HTTPException


def _sha512(data):
    return hashlib.sha512(data).hexdigest()


FAMILY_NAME = 'AirAnchor'
FAMILY_VERSION = '1.0'

LOCATION_KEY_ADDRESS_PREFIX = _sha512(
    FAMILY_NAME.encode('utf-8'))[:6]


def make_location_key_address(key, hash=None):
    prefix = LOCATION_KEY_ADDRESS_PREFIX + key[:6]
    
    if not hash:
        return prefix

    return prefix + hash[-58:]


def _get_private_key_as_signer(priv_path):
    context = create_context('secp256k1')
    crypto_factory = CryptoFactory(context=context)
    
    if priv_path != None:
        with open(priv_path, "r") as f:
            key_hex = f.read().strip()

        key = Secp256k1PrivateKey.from_hex(key_hex)
        
    else:
        key = context.new_random_private_key()
        
    return crypto_factory.new_signer(key)

def _validate_http_url(url: str):
    return 'http://' + url if not url.startswith("http://") else url


class Server:
    
    def __init__(self, sawtooth_rest_url, ca_url, priv_key_path=None):
        self._signer = _get_private_key_as_signer(priv_key_path)
        self._ca = _validate_http_url(ca_url)
        self._url = "{}/{}".format(_validate_http_url(sawtooth_rest_url), 'batches')

    
    def create_and_send_batch(self, tr: TransactionRequest):
        csr_firm = self._send_csr_firm_request(tr.csr)
        
        payload = self._create_payload(tr, csr_firm)
        
        self._send_batches(payload=payload, signer_key=tr.csr.public_key)
        
        # add row to mongo
    
    
    def _send_csr_firm_request(self, csr: CertificateSignedRequest):
        def validate_pub_key(pub_key):
            try:
                Secp256k1PublicKey.from_hex(pub_key)
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid public key")
            
        validate_pub_key(csr.public_key)
        ca_firm_url = '{}/{}'.format(self._ca, 'api/v1/sign')
                    
        try:
            ca_response = requests.post(ca_firm_url, json=csr.as_str())
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e
         
        if ca_response.status_code == 401:
            raise HTTPException(status_code=401, detail=ca_response.reason)
         
        if ca_response.status_code != 200:
            raise HTTPException(status_code=500, detail="There was an error trying to call ca. Reason: {}".format(ca_response.reason))
    
        return ca_response.json()
    
    
    def _create_payload(self, tr: TransactionRequest, csr_firm: str):
        csr_encoded = cbor.dumps(tr.csr.as_str())
        csr_hex = hexlify(csr_encoded).decode('ascii')
        
        encoded_nonce = secrets.token_hex(16)
        
        payload = {
            'csr': csr_hex,
            'csr_firm': csr_firm,
            'nonce': encoded_nonce,
            'data': tr.data
        }
                
        return cbor.dumps(payload)
    
    
    def _send_request(self, data):
        headers = {
            'Content-Typeo': 'application/octet-stream'
        }

        try:
            result = requests.post(self._url, headers=headers, data=data)

            if not result.ok:
                raise Exception("Error {}: {}".format(
                    result.status_code, result.reason))

        except requests.ConnectionError as err:
            raise HTTPException(status_code=500, 
                detail='Failed to connect to REST API: {}'.format(err)) from err

        except BaseException as err:
            raise HTTPException(status_code=500, detail=str(err)) from err

        return result.text


    def _send_batches(self, payload, signer_key):
        
        payload_sha512=_sha512(payload)
        batcher_key = self._signer.get_public_key().as_hex()

        # Construct the address
        address = make_location_key_address(batcher_key, payload_sha512)

        header = TransactionHeader(
            signer_public_key=signer_key,
            family_name=FAMILY_NAME,
            family_version=FAMILY_VERSION,
            inputs=[address],
            outputs=[address],
            dependencies=[],
            payload_sha512=payload_sha512,
            batcher_public_key=batcher_key,
            nonce=secrets.token_hex(16)
        ).SerializeToString()

        signature = self._signer.sign(header)

        transaction = Transaction(
            header=header,
            payload=payload,
            header_signature=signature
        )
        
        batch_list = self._create_batch_list([transaction])

        return self._send_request(batch_list.SerializeToString())
        
            
    def _create_batch_list(self, transactions):
        transaction_signatures = [t.header_signature for t in transactions]

        header = BatchHeader(
            signer_public_key=self._signer.get_public_key().as_hex(),
            transaction_ids=transaction_signatures
        ).SerializeToString()

        signature = self._signer.sign(header)

        batch = Batch(
            header=header,
            transactions=transactions,
            header_signature=signature)

        return BatchList(batches=[batch])