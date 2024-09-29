import sys
import signal
import argparse
import logging
import uuid

from multiprocessing import Process
from src.consumer import ConsumerEngine
from src.config import consumers_group_size, redis_host, redis_port

logging.basicConfig(level=logging.DEBUG)

# Running section

processes = []
def start_consumer(consumer_id):
    '''
    Start consumer

    :param consumer_id:
    :return:
    '''
    consumer = ConsumerEngine(consumer_id, redis_host, redis_port)
    consumer.listen_and_process()

def consumer_cleanup(signum, frame):
    """
    Cleanup function to stop all consumers

    signum: signal - keep
    frame: frame - keep
    """
    logging.info("Stopping all consumers...")
    for process in processes:
        process.terminate()
    sys.exit(0)

def create_consumer_group(group_size):
    """
    Run consumer group in separated processes

    :param group_size:
    """
    for _ in range(int(group_size)):
        consumer_id = str(uuid.uuid4())  # Уникальный ID для каждого потребителя
        process = Process(target=start_consumer, args=(consumer_id,))
        process.daemon = True  # Делаем процесс демоном, чтобы он корректно завершался
        process.start()
        processes.append(process)

    # Ждем завершения всех процессов
    for process in processes:
        process.join()


if __name__ == "__main__":
    # I will parse the command line arguments to allow the user to specify the number of consumers in the group
    parser = argparse.ArgumentParser(description="Конфигурация группы потребителей")
    parser.add_argument('--group_size', type=int, required=False, help="Количество потребителей в группе")
    args = parser.parse_args()

    # If it available in command line i will override default value
    if args.group_size:
        consumers_group_size = int(args.group_size)

    # Registrate signals to correct shutdown
    signal.signal(signal.SIGINT, consumer_cleanup)
    signal.signal(signal.SIGTERM, consumer_cleanup)

    # I starting ...
    create_consumer_group(consumers_group_size)




