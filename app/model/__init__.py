
from dataclasses import dataclass


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

@dataclass
class TransactionRequest:
    csr: CertificateSignedRequest
    data: str
    
    def as_str(self):
        return {
            'csr': self.csr.as_str(),
            'data': self.data
        }