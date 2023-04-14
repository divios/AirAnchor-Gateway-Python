
from core import Server
from model import TransactionRequest
from data import MongoRepo
from concurrent.futures import ThreadPoolExecutor

import traceback
from pika import BlockingConnection, ConnectionParameters

from enviroments import *
from core.exceptions import Sawtooth_back_pressure_exception, Sawtooth_invalid_transaction_format


def _create_rabbit_channel():
    print('Initializing rabbitmq at {}'.format(RABBITMQ_URL))
    rabbit_connection = BlockingConnection(ConnectionParameters(RABBITMQ_URL))
    rabbit_channel = rabbit_connection.channel()
    
    rabbit_channel.queue_declare(queue='sawtooth', durable=True)
    
    return rabbit_channel


rabbit_channel = _create_rabbit_channel()

mongoRepo = MongoRepo(mongo_url=MONGO_URL, 
                      mongo_database=MONGO_DATABASE, 
                      mongo_collection=MONGO_COLLECTION)

server = Server(priv_key_path=PRIV_KEY_PATH, 
                sawtooth_validator_url=SAWTOOTH_VALIDATOR_URL, 
                mongo_repo=mongoRepo,
                ca_url=CA_API_URL)

executor = ThreadPoolExecutor()

def _consumer_callback(ch, method, props, body):
    try:
        tr = TransactionRequest.from_bytes(body)
        server.create_and_send_batch(tr)
        ch.basic_ack(delivery_tag = method.delivery_tag)
        
    except Sawtooth_back_pressure_exception as e:
        print("Getting too many requests response from validator, requeuing message...")
        pass            # Not sending ack, hence message is requeue for later processing (backoff)
        
    except Exception as e:
        print("There was an exception trying to sending the batch: {}".format(e))    
        ch.basic_ack(delivery_tag = method.delivery_tag)  # do not requeue

rabbit_channel.basic_consume(queue='sawtooth', 
                             on_message_callback=_consumer_callback)

rabbit_channel.start_consuming()