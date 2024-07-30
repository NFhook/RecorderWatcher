import os
import time
import pika
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class RecordingHandler(FileSystemEventHandler):
    def __init__(self, delay=5):
        super().__init__()
        self.delay = delay
        self.modified_files = {}
        self.rabbitmq_host = '10.10.22.72'
        self.rabbitmq_port = 5672
        self.rabbitmq_user = 'root'
        self.rabbitmq_password = 'Rabbitmq@2024#'
        self.queue_name = 'file_paths'
        self.connection = None
        self.channel = None
        self.connect_to_rabbitmq()

    def connect_to_rabbitmq(self):
        while True:
            try:
                credentials = pika.PlainCredentials(self.rabbitmq_user, self.rabbitmq_password)
                parameters = pika.ConnectionParameters(
                    host=self.rabbitmq_host, 
                    port=self.rabbitmq_port, 
                    credentials=credentials,
                    heartbeat=60,
                    blocked_connection_timeout=300
                )
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.queue_name, durable=True)
                logging.info("Connected to RabbitMQ")
                break
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelError) as e:
                logging.error(f"Connection to RabbitMQ failed: {e}")
                time.sleep(3)

    def send_to_rabbitmq(self, message):
        while True:
            try:
                if not self.connection or self.connection.is_closed:
                    self.connect_to_rabbitmq()
                self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
                logging.info(f"Sent to RabbitMQ: {message}")
                break
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelError, pika.exceptions.StreamLostError) as e:
                logging.error(f"Failed to send message, reconnecting to RabbitMQ: {e}")
                self.connect_to_rabbitmq()
                time.sleep(3)

    def on_created(self, event):
        if not event.is_directory and (event.src_path.endswith('.wav') or event.src_path.endswith('.mp3')):
            self.modified_files[event.src_path] = (time.time(), os.path.getsize(event.src_path))

    def on_modified(self, event):
        if not event.is_directory and (event.src_path.endswith('.wav') or event.src_path.endswith('.mp3')):
            self.modified_files[event.src_path] = (time.time(), os.path.getsize(event.src_path))

    def check_files(self):
        current_time = time.time()
        for file_path, (last_modified, last_size) in list(self.modified_files.items()):
            try:
                current_size = os.path.getsize(file_path)
                if current_size == last_size:
                    logging.info("<< {} << Timer: {}".format(datetime.now(), current_time-last_modified))
                    if current_time - last_modified > self.delay:
                        abs_path = os.path.abspath(file_path)
                        logging.info(f"File write completed: {abs_path}")
                        self.send_to_rabbitmq(abs_path)
                        del self.modified_files[file_path]
                else:
                    self.modified_files[file_path] = (current_time, current_size)
            except FileNotFoundError:
                logging.warning(f"File not found (may have been moved or deleted): {file_path}")
                del self.modified_files[file_path]
        if self.connection and self.connection.is_open:
            self.connection.process_data_events()


def setup_logging():
    log_format = '%(asctime)s %(levelname)s %(message)s'
    formatter = logging.Formatter(log_format)

    log_handler = TimedRotatingFileHandler(datetime.now().strftime("logs/%Y-%m-%d.log"), when="midnight", interval=1)
    log_handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logger.addHandler(log_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

def get_current_date_path(base_path):
    today = datetime.today()
    return os.path.join(base_path, today.strftime('%Y-%m-%d'))

if __name__ == "__main__":
    setup_logging()
    path_to_watch = '/home/ccrecord/recordings/'
    event_handler = RecordingHandler(delay=5)
    observer = Observer()
    observer.schedule(event_handler, path=path_to_watch, recursive=True)
    observer.start()
    
    try:
        while True:
            time.sleep(0.5)
            event_handler.check_files()
    except KeyboardInterrupt:
        observer.stop()
    observer.join()