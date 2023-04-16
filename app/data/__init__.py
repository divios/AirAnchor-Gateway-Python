

from pymongo import MongoClient
from datetime import datetime

# {sender: str, signer: str, ca: str, hash:str, confirmed: date}
class MongoRepo:
    
    def __init__(self, mongo_url, mongo_database, mongo_collection):
        print('Itinializing mongo connection at {}'.format(mongo_url))
        self._client = MongoClient(mongo_url)
        self._db = self._client[mongo_database]
        self._collection = self._db[mongo_collection]
        
    
    def create(self, document):
        return self._collection.insert_one(document)
        
    def create_many(self, documents):
        return self._collection.insert_many(documents)
        
    def find(self, query):
        return self._collection.find(query)
    
    def find_by_hash(self, hash):
        return self._collection.find_one({'hash': hash})

    def find_all_by_sender(self, sender):
        return self._collection.find({'sender': sender})
    
    def set_confirmed(self, hash, confirmed_at):
        return self._collection.update_one({'hash': hash}, {'$set': {'confirmed_at': datetime.now()}})
    
    def delete(self, query):
        return self._collection.delete_many(query)