"""
    I'm a Consumers Manager, I will manage consumers and keep track of their activity.
    I will be responsible for managing the consumers and actualizing the list of active consumers.

"""

import time
import logging
import redis
import threading

logging.basicConfig(level=logging.DEBUG)

from config import redis_host, redis_port, stats_name, consumer_manager_ttl, consumer_manager_interval, consumer_ids


class ConsumerManager:
    def __init__(self):
        self.redis = redis.Redis(host=redis_host, port=redis_port)
        self.stats_name = stats_name
        self.ttl = consumer_manager_ttl
        self.consumers_set = consumer_ids
        self.update_interval = consumer_manager_interval
        self.running = False

    def update_last_activity(self, consumer_id):
        """
        Refresh the last activity of a consumer

        :param consumer_id: str Consumer ID

        """
        # My activity i will put here
        key = f"{self.stats_name}:{consumer_id}:last_activity"
        # I will set the time
        self.redis.set(key, int(time.time()))
        # I will add the consumer to the set to be ensured that i'm active
        self.redis.sadd(self.consumers_set, consumer_id)

    def is_active(self, consumer_id):
        """
        Check if a consumer is active

        :param consumer_id: str Consumer ID
        """
        # I will check if the consumer is active by this key
        key = f"{self.stats_name}:{consumer_id}:last_activity"
        # I check existing of the key
        last_activity = self.redis.get(key)
        # If not exist i will return False so the consumer is not active
        if last_activity is None:
            return False
        # Otherwise i will check the time
        last_activity = float(last_activity)
        current_time = time.time()

        # I will check if the time is more than the ttl from the last activity
        if current_time - last_activity > self.ttl:
            # If it's more i will return False and log that the consumer is inactive
            logging.info(f"[{int(current_time)}] Consumer {consumer_id} is inactive ...")
            return False
        # Otherwise i will return that the consumer is active
        return True

    def cleanup_inactive_consumers(self):
        """
        Remove inactive consumers from the list

        """
        # I will get the list of consumers from the set
        consumers = self.redis.smembers(self.consumers_set)
        # If the list is empty i will log that there is no active consumers
        if len(consumers) == 0:
            logging.info("No active consumers found.")

        # I will go over the consumers
        for consumer_id in consumers:
            # I will remember the count of removed consumers
            removed_items_count = 0
            # I will decode the consumer because it's stored as bytes (i not sure m.b. better some like Redis(decode_responses=True))
            consumer_id = consumer_id.decode()
            # I will check if the consumer is active
            if not self.is_active(consumer_id):
                # If not i will remove the consumer from the set
                removed_items_count += 1
                logging.info(f"[{removed_items_count}] Removing inactive consumer {consumer_id} ...")
                self.redis.srem(self.consumers_set, consumer_id)
            if removed_items_count > 0:
                # If i removed some consumers i will log summary
                logging.info(f"Removed {removed_items_count} inactive consumers.")

    def get_active_consumers(self):
        """
        Get the list of active consumers

        """
        # I remove inactive consumers from set
        self.cleanup_inactive_consumers()
        # and return the list of active consumers
        return self.redis.smembers(self.consumers_set)

    def run_service(self):
        """
        Run the service in the background
        This function will run in the background and update the list of active
        consumers every `update_interval` seconds.

        """
        itr = 0
        while self.running:
            # Nothing to comment ... It's my routine
            itr += 1
            logging.info(f"[{itr}] [{int(time.time())}] Updating active consumers list ...")
            self.cleanup_inactive_consumers()
            time.sleep(self.update_interval)

    def start_service(self):
        """
        Running my service in the background
        """
        if not self.running:
            self.running = True
            threading.Thread(target=self.run_service, daemon=True).start()
            print("[+] Service started.")

    def stop_service(self):
        """
        Остановка службы
        """
        self.running = False
        print("[-] Service stopped.")


if __name__ == "__main__":
    # This is my starting ...
    manager = ConsumerManager()
    manager.start_service()

    # I will run the service until the user stops it
    try:
        while True:
            # I will just resting while the service is working on the background.
            # ==> This pause needs to prevent leaks of resources (mostly CPU)
            time.sleep(1)
    except KeyboardInterrupt:
        manager.stop_service()
