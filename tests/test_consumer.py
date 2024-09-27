import unittest
import json
from unittest.mock import patch, MagicMock
from src.consumer import ConsumerEngine

class TestConsumerEngine(unittest.TestCase):

    @patch('redis.Redis')
    def setUp(self, mock_redis):
        # Mock Redis connection
        self.mock_redis = mock_redis.return_value
        self.consumer = ConsumerEngine(consumer_id='test_consumer', redis_host='localhost', redis_port=6379)

    def test_acquire_lock_success(self):
        # Test successful lock acquisition
        self.mock_redis.set.return_value = True
        message_id = "1F1B1E1C-1B1B-1E1B-1B1B-1B1E1B1B1E1B"
        lock_acquired = self.consumer.acquire_lock(message_id)
        self.assertTrue(lock_acquired)
        self.mock_redis.set.assert_called_with(f'{self.consumer.lock_name}:{message_id}', self.consumer.consumer_id, nx=True, ex=5)

    def test_acquire_lock_failure(self):
        # Test failure to acquire lock
        self.mock_redis.set.return_value = False
        message_id = "1F1B1E1C-1B1B-1E1B-1B1B-1B1E1B1B1E1B"
        lock_acquired = self.consumer.acquire_lock(message_id)
        self.assertFalse(lock_acquired)

    @patch('json.loads')
    def test_listen_and_process_message(self, mock_json_loads):
        # Mock the pubsub listening process
        pubsub_mock = MagicMock()
        pubsub_mock.listen.return_value = [
            {'type': 'message', 'data': json.dumps({'message_id': '1F1B1E1C-1B1B-1E1B-1B1B-1B1E1B1B1E1B'}).encode('utf-8')},
        ]
        self.mock_redis.pubsub.return_value = pubsub_mock
        self.mock_redis.set.return_value = True  # Lock acquisition success
        mock_json_loads.return_value = {'message_id': '1F1B1E1C-1B1B-1E1B-1B1B-1B1E1B1B1E1B'}

        with patch.object(self.consumer, 'process_message') as mock_process_message:
            self.consumer.listen_and_process()
            mock_process_message.assert_called_once_with({'message_id': '1F1B1E1C-1B1B-1E1B-1B1B-1B1E1B1B1E1B'})


    @patch('json.loads')
    def test_listen_and_process_lock_acquisition_failure(self, mock_json_loads):
        # Test failure to acquire lock in listen_and_process
        pubsub_mock = MagicMock()
        pubsub_mock.listen.return_value = [
            {'type': 'message', 'data': bytes(str({'message_id': '1F1B1E1C-1B1B-1E1B-1B1B-1B1E1B1B1E1B'}).encode())},
        ]
        self.mock_redis.pubsub.return_value = pubsub_mock
        self.mock_redis.set.return_value = False  # Lock acquisition failure
        mock_json_loads.return_value = {'message_id': '1F1B1E1C-1B1B-1E1B-1B1B-1B1E1B1B1E1B'}

        with patch.object(self.consumer, 'process_message') as mock_process_message:
            self.consumer.listen_and_process()
            mock_process_message.assert_not_called()

    @patch('random.random', return_value=0.5)
    def test_process_message(self, mock_random):
        # Test message processing and streaming to Redis Stream
        message = {'message_id': '123'}
        self.consumer.process_message(message)

        # Check if additional fields were added to the message
        self.assertEqual(message['processed_by'], 'test_consumer')
        self.assertEqual(message['random_property'], 0.5)

        # Ensure data was streamed to Redis
        self.mock_redis.xadd.assert_called_once_with(self.consumer.stream_name, {
            'message_id': '123',
            'processed_by': 'test_consumer',
            'processed_message': json.dumps(message),
            'created_at': unittest.mock.ANY
        })


    @patch('sys.exit')
    def test_shutdown(self, mock_exit):
        # Test graceful shutdown
        self.consumer.shutdown(signum=1, frame=None)
        self.mock_redis.srem.assert_called_once_with("consumer:ids", self.consumer.consumer_id)
        self.mock_redis.close.assert_called_once()
        mock_exit.assert_called_once_with(0)


