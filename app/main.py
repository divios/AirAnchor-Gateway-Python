
from core import Server
from model import TransactionRequest
from data import MongoRepo
from concurrent.futures import ThreadPoolExecutor

import traceback
from pika import BlockingConnection, ConnectionParameters

from enviroments import *


def _create_rabbit_channel():
    print('Initializing rabbitmq at {}'.format(RABBITMQ_URL))
    rabbit_connection = BlockingConnection(ConnectionParameters(RABBITMQ_URL))
    rabbit_channel = rabbit_connection.channel()
    
    rabbit_channel.queue_declare(queue='sawtooth', durable=True)
    
    return rabbit_channel


rabbit_channel = _create_rabbit_channel()

mongoRepo = MongoRepo(mongo_url=MONGO_URL, 
                      mongo_database=MONGO_DATABASE, mongo_collection=MONGO_COLLECTION)

server = Server(priv_key_path=PRIV_KEY_PATH, sawtooth_validator_url=SAWTOOTH_VALIDATOR_URL, 
                mongo_repo=mongoRepo, ca_url=CA_API_URL)

def _consumer_callback(u, b, props, body):
    try:
        tr = TransactionRequest.from_bytes(body)
        server.create_and_send_batch(tr)
    except Exception as e:
        traceback.print_exc()

rabbit_channel.basic_consume(queue='sawtooth', 
                             on_message_callback=_consumer_callback, 
                             auto_ack=True)

rabbit_channel.start_consuming()