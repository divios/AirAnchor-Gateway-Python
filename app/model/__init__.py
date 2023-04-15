
from dataclasses import dataclass, asdict
import cbor
import hashlib
import json
from functools import lru_cache

@dataclass(frozen=True)
class CertificateSignedRequest:
    distinguished_name: str
    public_key: str
    optional_params: dict = None
    
    def as_str(self):
        return {
            'distinguished_name': self.distinguished_name,
            'public_key': self.public_key,
            'optional_params': self.optional_params
        }
        
    @staticmethod
    def from_dict(dict: dict):
        attributes = ['distinguished_name', 'public_key', 'optional_params']
        
        if not all([attr in attributes for attr in dict.keys()]):
            raise Exception("Malformed Certificate request")
        
        return CertificateSignedRequest(dict['distinguished_name'], dict['public_key'], dict['optional_params'])

@dataclass(frozen=True)
class TransactionRequest:
    sender_public_key: str
    csr: CertificateSignedRequest
    data: str
    
    def as_dict(self):
        return asdict(self)
        
    def serialize(self) -> str:
        return cbor.dumps(self.as_dict())
        
    @staticmethod
    def from_dict(dict: dict):
        attributes = ['sender_public_key', 'csr', 'data']
        
        if not all([attr in attributes for attr in dict.keys()]):
            raise Exception('Malformed transaction request')
        
        return TransactionRequest(dict['sender_public_key'], CertificateSignedRequest.from_dict(dict['csr']), dict['data'])
        
    @staticmethod
    def deserialize(encoded):
        try:
            dict = cbor.loads(encoded)
        except Exception as e:
            raise Exception("Malformed transaction request bytes") from e
        
        return TransactionRequest.from_dict(dict)
    
    
@dataclass(frozen=True)
class TransactionPayload:
    csr: str
    csr_firm: str
    pub_key: str
    nonce: str
    data: str

    def as_dict(self):
        return asdict(self)

    def serialize(self):
        return cbor.dumps(self.as_dict())
    
    def hash(self):
        return hashlib.sha512(
            self.serialize()).hexdigest()