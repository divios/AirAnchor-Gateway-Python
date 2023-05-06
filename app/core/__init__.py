

from threading import Thread
from pika import BlockingConnection, ConnectionParameters
import requests
import hashlib
import random
import os
import cbor
import secrets
import asyncio

from enviroments import *

from binascii import hexlify
from time import time, monotonic, sleep

from model import TransactionRequest, CertificateRequest, TransactionPayload
from data import MongoRepo

from sawtooth_signing.secp256k1 import Secp256k1PublicKey
from sawtooth_sdk.messaging.stream import Stream
from sawtooth_signing import create_context
from sawtooth_signing import CryptoFactory
from sawtooth_signing import ParseError
from sawtooth_signing.secp256k1 import Secp256k1PrivateKey

from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader
from sawtooth_sdk.protobuf.transaction_pb2 import Transaction
from sawtooth_sdk.protobuf.validator_pb2 import Message
from sawtooth_sdk.protobuf.batch_pb2 import BatchList
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader
from sawtooth_sdk.protobuf.batch_pb2 import Batch
from sawtooth_sdk.protobuf.client_batch_submit_pb2 import ClientBatchSubmitResponse

from core.exceptions import Sawtooth_back_pressure_exception, Sawtooth_invalid_transaction_format
from fastapi import HTTPException

def _sha512(data):
    return hashlib.sha512(data).hexdigest()


FAMILY_NAME = 'AirAnchor'
FAMILY_VERSION = '1.0'

LOCATION_KEY_ADDRESS_PREFIX = _sha512(
    FAMILY_NAME.encode('utf-8'))[:6]

CONTEXT = create_context('secp256k1')

def make_location_key_address(key, hash=None):
    prefix = LOCATION_KEY_ADDRESS_PREFIX + key[:6]
    
    if not hash:
        return prefix

    return prefix + hash[-58:]


def _get_private_key_as_signer(priv_path):
    crypto_factory = CryptoFactory(context=CONTEXT)
    
    if priv_path != None:
        with open(priv_path, "r") as f:
            key_hex = f.read().strip()

        key = Secp256k1PrivateKey.from_hex(key_hex)
        
    else:
        key = CONTEXT.new_random_private_key()
        
    return crypto_factory.new_signer(key)

def _validate_http_url(url: str):
    return 'http://' + url if not url.startswith("http://") else url


def _validate_tcp_url(url: str):
    return 'tcp://' + url if not url.startswith("tcp://") else url


class Server:
    
    def __init__(self, rabbit_connection, sawtooth_validator_url: str, ca_url: str, mongo_repo: MongoRepo, priv_key_path=None):
        self.rabbit_connection = rabbit_connection
        self._signer = _get_private_key_as_signer(priv_key_path)
        self._ca = _validate_http_url(ca_url)
        
        url = _validate_tcp_url(sawtooth_validator_url)
        print("Initilizating zmq in {}".format(url))
        self._connection = Stream(url)
        
        self._mongo_repo = mongo_repo

    
    def create_and_send_batch(self, trs: list[TransactionRequest]):
        print("Receiving petition to send batch request: {}".format(trs))
        
        transactions = []
        transactions_payload = []
        for tr in trs:
            if not self._validate_transaction_request(tr):
                continue
            
            certificate_signature = self._send_csr_firm_request(tr.header.certificate_request)
            
            payload = self._create_payload(tr, certificate_signature)
            transaction = self._wrap_payload_in_transaction(payload)
            
            transactions_payload.append(payload)
            transactions.append(transaction)
        
        batches = self._merge_transactions_to_batches(transactions)
        
        status = self._send_batches(batches)
        
        self._start_blockchain_event_callback_listener(transactions_payload)
              
        print("Resolved as {}".format(status))
    
    
    def _validate_transaction_request(self, tr: TransactionRequest):
        try:
            pub_key = Secp256k1PublicKey.from_hex(tr.header.sender_public_key)
        except Exception:
            print("Invalid public key")
            return False
            
        if not CONTEXT.verify(tr.signature, tr.header.serialize(), pub_key):
            print('Invalid signature')
            return False
        
        return True
       
    
    def _send_csr_firm_request(self, csr: CertificateRequest):
        ca_firm_url = '{}/{}'.format(self._ca, 'api/v1/sign')
                    
        try:
            ca_response = requests.post(ca_firm_url, json=csr.as_dict())
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e
         
        if ca_response.status_code == 401:
            raise HTTPException(status_code=401, detail=ca_response.reason)
         
        if ca_response.status_code != 200:
            raise HTTPException(status_code=500, detail="There was an error trying to call ca. Reason: {}".format(ca_response.reason))
    
        return ca_response.json()
    
    
    def _create_payload(self, tr: TransactionRequest, csr_firm: str) -> TransactionPayload:
        return TransactionPayload.create(
            signer=self._signer,
            certificate_request=tr.header.certificate_request,
            certificate_authority_signature=csr_firm,
            data=tr.data
        )
    
    
    def _wrap_payload_in_transaction(self, payload: TransactionPayload) -> Transaction:
        payload_hash = payload.hash()
        batcher_key = self._signer.get_public_key().as_hex()
        sender_key = payload.sender_public_key
        
        address = make_location_key_address(sender_key, payload_hash)

        header = TransactionHeader(
            signer_public_key=batcher_key,
            family_name=FAMILY_NAME,
            family_version=FAMILY_VERSION,
            inputs=[address],
            outputs=[address],
            dependencies=[],
            payload_sha512=payload_hash,
            batcher_public_key=batcher_key,
            nonce=secrets.token_hex(16)
        ).SerializeToString()

        signature = self._signer.sign(header)

        transaction = Transaction(
            header=header,
            payload=payload.serialize(),
            header_signature=signature
        )
                
        return transaction
     
                
    def _merge_transactions_to_batches(self, transactions) -> BatchList:
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
    
    
    def _send_batches(self, batches: BatchList):
        # Submit the batch list to the validator
        future = self._connection.send(
            message_type=Message.CLIENT_BATCH_SUBMIT_REQUEST,
            content=batches.SerializeToString())
        
        response = future.result(timeout=5)
        response_proto = self._parse_batch_response(response)
        
        final_status = self._validate_batch_response_status(response_proto)
        
        return final_status

    
    def _send_request(self, data):
        headers = {
            'Content-Type': 'application/octet-stream'
        }

        try:
            result = requests.post(self._ca, headers=headers, data=data)

            print(result.json())

            if not result.ok:
                raise Exception("Error {}: {}".format(
                    result.status_code, result.reason))

        except requests.ConnectionError as err:
            raise HTTPException(status_code=500, 
                detail='Failed to connect to REST API: {}'.format(err)) from err

        except BaseException as err:
            raise HTTPException(status_code=500, detail=str(err)) from err

        return result.text

    
    def _parse_batch_response(self, response):
        content = ClientBatchSubmitResponse()
        content.ParseFromString(response.content)
        return content
    
    
    def _validate_batch_response_status(self, proto_response):        
        validator_map = {
            'INVALID_BATCH': Sawtooth_invalid_transaction_format,
            'QUEUE_FULL': Sawtooth_back_pressure_exception
        }
        
        proto_status = ClientBatchSubmitResponse.Status.Name(proto_response.status)

        validation = validator_map.get(proto_status, None)
        
        if validation:
            raise validation()
        
        return proto_status
    
    
    def _start_blockchain_event_callback_listener(self, transactions_payload: list[TransactionPayload]):
        def _async_task(hash):            
            connection = BlockingConnection(ConnectionParameters(host=RABBITMQ_URL,
                                                                blocked_connection_timeout=30)) 
            
            channel = connection.channel()
            channel.queue_declare(queue=hash, durable=False)
            
            result = False
            start_time = monotonic()
            while monotonic() - start_time < 30:
                
                response = channel.basic_get(queue=hash, auto_ack=True)
                
                if all(x is not None for x in response):            # Nos han respondido
                    result = True
                    break
            
                sleep(0.4)
            
            if result:
                self._save_mongo_document(payload)
                print("Result for hash {} confirmed!".format(hash))
            else:
                print("Result for hash {} was not confirmed".format(hash))
            
            channel.queue_delete(queue=hash)
            
            channel.close()
            connection.close()
                
            
        for payload in transactions_payload:
            hash = payload.hash()
            
            consume_thread = Thread(target=_async_task, args=[hash])
            consume_thread.start()
            
        
    def _save_mongo_document(self, payload: TransactionPayload):
        document = {
            'sender': payload.sender_public_key,
            'signer': self._signer.get_public_key().as_hex(),
            'ca': '',
            'hash': payload.hash()
        }
    
        self._mongo_repo.create(document)