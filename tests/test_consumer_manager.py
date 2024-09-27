import unittest
import os
import sys
from unittest.mock import patch, MagicMock
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from consumer_manager import ConsumerManager

sys.modules['config'] = MagicMock()
sys.modules['config'].redis_host = 'localhost'
sys.modules['config'].redis_port = 6379
sys.modules['config'].stats_name = 'test_stats'
sys.modules['config'].consumer_manager_ttl = 60
sys.modules['config'].consumer_manager_interval = 5
sys.modules['config'].consumer_ids = 'test_consumers'


class TestConsumerManager(unittest.TestCase):

    @patch('src.consumer_manager.redis.Redis')
    def setUp(self, MockRedis):
        self.mock_redis = MockRedis.return_value
        self.manager = ConsumerManager()

    def test_update_last_activity(self):
        consumer_id = "consumer_1"
        current_time = int(time.time())

        self.manager.update_last_activity(consumer_id)

        key = f"{self.manager.stats_name}:{consumer_id}:last_activity"
        self.mock_redis.set.assert_called_once_with(key, current_time)
        self.mock_redis.sadd.assert_called_once_with(self.manager.consumers_set, consumer_id)

    def test_is_active_consumer_active(self):
        consumer_id = "cnsmr_1"
        last_activity = int(time.time()) - (self.manager.ttl - 10)

        self.mock_redis.get.return_value = str(last_activity).encode()
        self.assertTrue(self.manager.is_active(consumer_id))

    def test_is_active_consumer_inactive(self):
        consumer_id = "cnsmr_1"
        last_activity = int(time.time()) - (self.manager.ttl + 10)

        self.mock_redis.get.return_value = str(last_activity).encode()
        self.assertFalse(self.manager.is_active(consumer_id))

    def test_is_active_consumer_nonexistent(self):
        consumer_id = "cnsmr_1"
        self.mock_redis.get.return_value = None
        self.assertFalse(self.manager.is_active(consumer_id))

    def test_cleanup_inactive_consumers(self):
        consumer_id_active = "cnsmr_1"
        consumer_id_inactive = "cnsmr_2"
        last_activity_active = int(time.time())
        last_activity_inactive = int(time.time()) - (self.manager.ttl + 10)

        self.mock_redis.smembers.return_value = [consumer_id_active.encode(), consumer_id_inactive.encode()]
        self.mock_redis.get.side_effect = [str(last_activity_active).encode(), str(last_activity_inactive).encode()]

        self.manager.cleanup_inactive_consumers()

        self.mock_redis.srem.assert_called_once_with(self.manager.consumers_set, consumer_id_inactive)

    def test_get_active_consumers(self):
        with patch.object(self.manager, 'cleanup_inactive_consumers', return_value=None) as mock_cleanup:
            self.mock_redis.smembers.return_value = {b'consumer_1', b'consumer_2'}

            active_consumers = self.manager.get_active_consumers()
            self.assertEqual(active_consumers, {b'consumer_1', b'consumer_2'})
            mock_cleanup.assert_called_once()

    def test_start_stop_service(self):
        with patch('threading.Thread') as mock_thread:
            self.manager.start_service()
            self.assertTrue(self.manager.running)
            mock_thread.assert_called_once()

        self.manager.stop_service()
        self.assertFalse(self.manager.running)
