"""
    Consumer Class

    Hi-scalable consumer class for processing messages from Redis Pub/Sub

    Attributes:
    -----------
    consumer_id : str
        Unique consumer ID
    pubsub_channel : str
        Pub/Sub channel name
    stream_name : str
        Redis Stream name
    stats_name : str
        Redis key for consumer stats
    r : redis.Redis
        Redis connection object


    Connectivity:

    - Redis
        :param
            host : str
                Redis host
            port : int
                Redis port

    - Signal - catch signals for graceful shutdown
        :signal
            SIGINT
            SIGTERM
"""

import signal
import threading
import time
import sys

import redis
import json
import random
import logging

from src.config import stream_name, pubsub_channel, stats_name, lock_name, consumer_ids

# I will show ALL HAPPENING in my life
DEBUG = False

class ConsumerEngine:
    def __init__(self, consumer_id: str, redis_host: str, redis_port: str) -> None:
        # Hello, Redis! Connection to Redis (Sorry, simplest way)
        self.r = redis.Redis(host=redis_host, port=redis_port)
        # My Unique ID for self instance of consumer
        self.consumer_id = consumer_id
        # as a consumer i will subscribe to this channel
        self.pubsub_channel = pubsub_channel
        # all processed messages i will store in this stream
        self.stream_name = stream_name
        # i think is better to collect some additional information.
        # For example get some stats, like count of processed messages and last_activity time
        self.stats_name = stats_name
        # I will use locking mechanism for message processing and here will be my lock's
        self.lock_name = lock_name
        # I will register myself on connection.
        self.consumer_ids = consumer_ids
        # I will be keep to be alive
        self.keep_alive_thread = threading.Thread(target=self.keep_alive, daemon=True)
        # A was born ...
        self.keep_alive_thread.start()

        # I hope for the best, but prepare for the worst
        # Register signal handlers for shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)


    def shutdown(self, signum, frame) -> None:
        """
        Graceful shutdown

        note: please keep unused variables in the function signature.
        In needs to be there for signal handler
        """
        # She don't like me ...
        if DEBUG:
            logging.info(f"Consumer {self.consumer_id} stopping ...")
        # Exactly, i will remove myself from the list of active consumers
        self.r.srem("consumer:ids", self.consumer_id)
        # And I will close Redis connection
        self.r.close()
        # Bang Bang - i kill myself ...
        sys.exit(0)

    def acquire_lock(self, message_id) -> bool:
        """
        Acquire redis-based lock for message processing

        :param message_id:
        :return:
        """
        # I will use lock name with message_id as a unique identifier.
        _lock_name = f"{self.lock_name}:{message_id}"
        # i decide that [ex=5 and nx=True] is more that enough because i will do it only if lock is not exists
        # so i will set this lock for 5 seconds. If my colleague will try to acquire it also, he will skip this message
        return self.r.set(_lock_name, self.consumer_id, nx=True, ex=5)

    def listen_and_process(self) -> None:
        """
        I will listen to Pub/Sub channel and process messages, if i can acquire lock on some message

        :return:
        """
        # I will use Pub/Sub for listening to messages
        pubsub = self.r.pubsub()
        # I will subscribe to Pub/Sub channel
        pubsub.subscribe(self.pubsub_channel)

        logging.info(f"Consumer {self.consumer_id} subscribed {self.pubsub_channel}")

        # I listen ether while i am alive
        for message in pubsub.listen():
            # I will process only messages
            if message['type'] == 'message':
                # I will try to parse message data
                data = json.loads(message['data'].decode('utf-8'))
                # I will get message_id
                message_id = data.get("message_id")
                # As a good boy i will try to acquire lock for message and process it on success
                if self.acquire_lock(message_id):
                    if DEBUG:
                        logging.info(f"Consumer {self.consumer_id} acquired lock for message {message_id}")
                    self.process_message(data)

                    # I will count processed messages
                    _counting_processed = f"{self.stats_name}:{self.consumer_id}:processed_messages"
                    self.r.incr(_counting_processed)

                    # I will update last_activity time
                    _updating_last_processed = f"{self.stats_name}:{self.consumer_id}:last_activity"
                    self.r.set(_updating_last_processed, time.time(), ex=60)

    def process_message(self, message) -> None:
        """
        Process message and stream it to Redis Stream

        :param message:
        :return:

        """

        # I will add some additional information to message about myself, because i was processed it
        message['processed_by'] = self.consumer_id
        # and I will add some random property
        message['random_property'] = random.random()

        # I will send data to Redis Stream according to my life rules
        self.r.xadd(self.stream_name, {
            "message_id": message["message_id"],
            "processed_by": self.consumer_id,
            "processed_message": json.dumps(message),
            "created_at": time.time()
        })

        if DEBUG:
            logging.info(f"Message {message['message_id']} was processed by: {self.consumer_id} and streamed")


    def keep_alive(self) -> None:
        """
        I will keep alive myself

        :return:
        """
        logging.info(f"Consumer {self.consumer_id} was {('register' if self.r.sadd(self.consumer_ids, self.consumer_id) else 'unregistred')}")
        while True:
            # I will update last_activity time
            _updating_last_processed = f"{self.stats_name}:{self.consumer_id}:last_activity"
            self.r.set(_updating_last_processed, time.time(), ex=60)
            # each 10 seconds
            time.sleep(10)





