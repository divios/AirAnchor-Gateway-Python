
from core import Server
from utils.TokenBucket import TokenBucket
from model import TransactionRequest, TransactionPayload
from data import MongoRepo
from threading import Thread
from pyrate_limiter import Duration, RequestRate, Limiter

import queue
import time

import traceback
from pika import BlockingConnection, ConnectionParameters

from enviroments import *
from core.exceptions import Sawtooth_back_pressure_exception


def _create_rabbit_channel():
    print('Initializing rabbitmq at {}'.format(RABBITMQ_URL))
    rabbit_connection = BlockingConnection(ConnectionParameters(host=RABBITMQ_URL,
                                                                blocked_connection_timeout=30))
    rabbit_channel = rabbit_connection.channel()
    
    rabbit_channel.queue_declare(queue='sawtooth', durable=True)
    
    return rabbit_connection, rabbit_channel


rabbit_connection, rabbit_channel = _create_rabbit_channel()

mongoRepo = MongoRepo(mongo_url=MONGO_URL, 
                      mongo_database=MONGO_DATABASE, 
                      mongo_collection=MONGO_COLLECTION)

server = Server(rabbit_connection=rabbit_connection,
                priv_key_path=PRIV_KEY_PATH, 
                sawtooth_validator_url=SAWTOOTH_VALIDATOR_URL, 
                mongo_repo=mongoRepo,
                ca_url=CA_API_URL)


token_bucket = TokenBucket(rate=TOKEN_RATE, capacity=TOKEN_CAPACITY) 
leaky_bucket = Limiter(RequestRate(limit=LEAKY_BUCKET_LIMIT, interval=Duration.SECOND))
buffer = queue.Queue(maxsize=TOKEN_CAPACITY)

remaining = 0

def consume_queue():
    def ack_batch(messages):
        ch, method, *ignore = messages[-1]  # take last message
        cb = lambda: ch.basic_ack(delivery_tag=method.delivery_tag, multiple=True)  # ack last with multiple, validating all the previous
        
        ch.connection.add_callback_threadsafe(cb)
        
    def reject_batch(messages, requeue=False):
            for ch, method, *ignore in messages:     # Reject all messages, hence message is requeue for later processing (backoff)
                ch.basic_reject(delivery_tag = method.delivery_tag, requeue=requeue)
                
    def wait_to_consume():
        global remaining
        tokens = 0

        while (tokens:= buffer.qsize()) == 0:                                  # sleep until message arrives
            time.sleep(0.2)
                                    
        while not token_bucket.consume(num_tokens=tokens):               # Wait for enough tokens
            pass
        
        if tokens > LEAKY_BUCKET_LIMIT:                                        # Remove excess and save it to remaining
            remaining += tokens - LEAKY_BUCKET_LIMIT
            tokens = LEAKY_BUCKET_LIMIT
        else:                                                                  # If we are behind limit, add remaining if possible
            excess = min(remaining, LEAKY_BUCKET_LIMIT - tokens)
            remaining -= excess
            tokens += excess
                                 
        return tokens
    
    def get_buffer_messages(tokens):
        messages = []
        
        i = 0                                               # take only tokens saw before
        while not buffer.empty() and i < tokens:
            messages.append(buffer.get())
            i += 1
        
        return messages
        
    def map_messages_to_payloads(messages):
        payloads = []
        for ch, method, body in messages:
            try:
                payloads.append(TransactionRequest.deserialize(body))
            except Exception as e:
                reject_batch(messages=[(ch, method, body)], requeue=False)
            
        return payloads
        
    while True:
        tokens = wait_to_consume()
        messages = get_buffer_messages(tokens)
        payloads = map_messages_to_payloads(messages)
       
        try:
            server.create_and_send_batch(payloads)         
            ack_batch(messages)
            
        except Sawtooth_back_pressure_exception as e:
            print("Getting too many requests response from validator, requeuing message...")
            
            reject_batch(messages=messages, requeue=True)    # Reject all messages but requeue, hence message is processed later

        except Exception as e:
            print("There was an exception sending the batch: {}".format(e))    
            traceback.print_exc()
            reject_batch(messages)
    
    
def _consumer_callback(ch, method, props, body):
    print('Received message: {}'.format(body))
    try:
        buffer.put((ch, method, body), block=True, timeout=1.2)
    except queue.Full:
        cb = lambda: ch.basic_reject(delivery_tag = method.delivery_tag, requeue=True)       # Requeue if buffer is full
        
        ch.connection.add_callback_threadsafe(cb)
                
        
consume_thread = Thread(target=consume_queue)
consume_thread.start()

rabbit_channel.basic_qos(prefetch_count=5)
rabbit_channel.basic_consume(queue='sawtooth', 
                             on_message_callback=_consumer_callback,
                             auto_ack=False
                             )

rabbit_channel.start_consuming()