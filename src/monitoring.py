"""
   I'm the bot that monitors the number of processed messages.
   My parents Consumer Module and Redis makes me just for the monitoring and nothing else.
   They just got tired of keeping everything in their heads and decided that now it's my job.
   After all, I'm their son ... :)

"""

import logging
import time
import redis

from config import redis_host, redis_port

logging.basicConfig(level=logging.DEBUG)

def monitor_processed_messages(seconds:int = 3):
    """Мониторинг количества обработанных сообщений"""
    r = redis.Redis(host=redis_host, port=redis_port)
    last_time = time.time()
    last_count = 0

    itr = 0
    while True:
        time.sleep(seconds)
        itr += 1
        current_time = time.time()
        try:
            messages_count = r.xlen("messages:processed")
            rate = (messages_count - last_count) / (current_time - last_time)
            logging.info(f"[{itr}] Processed {messages_count} messages, speed: {rate:.2f} msg/sec.")
        except:
            logging.warn("Not started yet")
            continue
        last_time = current_time
        last_count = messages_count


if __name__ == "__main__":
    monitor_processed_messages()