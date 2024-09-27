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
    """Запуск одного потребителя"""
    consumer = ConsumerEngine(consumer_id, redis_host, redis_port)
    consumer.listen_and_process()

def consumer_cleanup(signum, frame):
    """Удаление всех потребителей из списка при завершении"""
    logging.info("Stopping all consumers...")
    for process in processes:
        process.terminate()
    sys.exit(0)

def create_consumer_group(group_size):
    """Создаем группу потребителей и запускаем их в разных процессах"""
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
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description="Конфигурация группы потребителей")
    parser.add_argument('--group_size', type=int, required=False, help="Количество потребителей в группе")
    args = parser.parse_args()

    if args.group_size:
        consumers_group_size = int(args.group_size)

    # Регистрация обработчика сигнала
    signal.signal(signal.SIGINT, consumer_cleanup)
    signal.signal(signal.SIGTERM, consumer_cleanup)

    create_consumer_group(consumers_group_size)




