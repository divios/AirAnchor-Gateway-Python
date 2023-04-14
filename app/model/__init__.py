
from dataclasses import dataclass
import cbor

@dataclass
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

@dataclass
class TransactionRequest:
    sender_public_key: str
    csr: CertificateSignedRequest
    data: str
    
    def as_str(self):
        return {
            'csr': self.csr.as_str(),
            'data': self.data
        }
        
    @staticmethod
    def from_dict(dict: dict):
        attributes = ['sender_public_key', 'csr', 'data']
        
        if not all([attr in attributes for attr in dict.keys()]):
            raise Exception('Malformed transaction request')
        
        return TransactionRequest(dict['sender_public_key'], CertificateSignedRequest.from_dict(dict['csr']), dict['data'])
        
    @staticmethod
    def from_bytes(encoded):
        try:
            dict = cbor.loads(encoded)
        except Exception as e:
            raise Exception("Malformed transaction request bytes") from e
        
        return TransactionRequest.from_dict(dict)